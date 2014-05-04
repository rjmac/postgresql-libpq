{-# LANGUAGE ForeignFunctionInterface   #-}
{-# LANGUAGE EmptyDataDecls             #-}
{-# LANGUAGE BangPatterns               #-}

module Database.PostgreSQL.LibPQ.Internal
    (
     Connection(..)
    ,PGconn
    ,Result(..)
    ,PGresult
    ,withConn
    ,enumFromConn
    ,withResult
    ,AsyncStatus(..)
    ,getAsyncStatus
    ,setAsyncStatus
    ,setErrorMessage
    ,setResultError
    )
where

import Foreign
import Foreign.C.Types
import Foreign.C.String
import Data.ByteString as B
import Data.ByteString.Internal (c_strlen, memcpy)
import Control.Monad (when, unless)

import Database.PostgreSQL.LibPQ.PQExpBuffer

#include <internal/libpq-int.h>

-- | 'Connection' encapsulates a connection to the backend.
newtype Connection = Conn (ForeignPtr PGconn) deriving Eq
data PGconn

-- | 'Result' encapsulates the result of a query (or more precisely,
-- of a single SQL command --- a query string given to 'sendQuery' can
-- contain multiple commands and thus return multiple instances of
-- 'Result'.
newtype Result = Result (ForeignPtr PGresult) deriving (Eq, Show)
data PGresult

data AsyncStatus
   = AsyncIdle
   | AsyncBusy
   | AsyncReady
   | AsyncCopyIn
   | AsyncCopyOut
   | AsyncCopyBoth deriving (Eq, Show)

instance Enum AsyncStatus where
    toEnum (#const PGASYNC_IDLE)      = AsyncIdle
    toEnum (#const PGASYNC_BUSY)      = AsyncBusy
    toEnum (#const PGASYNC_READY)     = AsyncReady
    toEnum (#const PGASYNC_COPY_IN)   = AsyncCopyIn
    toEnum (#const PGASYNC_COPY_OUT)  = AsyncCopyOut
    toEnum (#const PGASYNC_COPY_BOTH) = AsyncCopyBoth
    toEnum _ = error "Database.Postgresql.LibPQ.Internal.AsyncStatus.toEnum: bad argument"

    fromEnum AsyncIdle     = (#const PGASYNC_IDLE)
    fromEnum AsyncBusy     = (#const PGASYNC_BUSY)
    fromEnum AsyncReady    = (#const PGASYNC_READY)
    fromEnum AsyncCopyIn   = (#const PGASYNC_COPY_IN)
    fromEnum AsyncCopyOut  = (#const PGASYNC_COPY_OUT)
    fromEnum AsyncCopyBoth = (#const PGASYNC_COPY_BOTH)

cintFromEnum :: (Enum a) => a -> CInt
cintFromEnum = fromIntegral . fromEnum

getAsyncStatus :: Connection -> IO AsyncStatus
getAsyncStatus conn = enumFromConn conn $ \ptr ->
  (#{peek struct pg_conn, asyncStatus} ptr) :: IO CInt

setAsyncStatus :: Connection -> AsyncStatus -> IO ()
setAsyncStatus conn stat = withConn conn $ \ptr ->
  #{poke struct pg_conn, asyncStatus} ptr (cintFromEnum stat)

withConn :: Connection
         -> (Ptr PGconn -> IO b)
         -> IO b
withConn (Conn !fp) f = withForeignPtr fp f

enumFromConn :: (Integral a, Enum b) => Connection
             -> (Ptr PGconn -> IO a)
             -> IO b
enumFromConn connection f = fmap (toEnum . fromIntegral) $ withConn connection f

withResult :: Result
           -> (Ptr PGresult -> IO b)
           -> IO b
withResult (Result fp) f = withForeignPtr fp f

setErrorMessage :: Connection -> B.ByteString -> IO ()
setErrorMessage conn text =
  withConn conn $ \connPtr ->
    replacePQExpBuffer (#{ptr struct pg_conn, errorMessage} connPtr) text

setResultError :: Result -> Maybe B.ByteString -> IO ()
setResultError r e =
  withResult r $ \rPtr ->
    case e of
      Just bytestring ->
        B.useAsCString bytestring $ \ePtr -> do
          dup <- resultStrdup rPtr ePtr
          #{poke struct pg_result, errMsg} rPtr dup
      Nothing ->
        #{poke struct pg_result, errMsg} rPtr nullPtr

resultStrdup :: Ptr PGresult -> CString -> IO CString
resultStrdup res str = do
  len <- c_strlen str
  newBytes <- resultAlloc res (len + 1) False
  unless (newBytes == nullPtr) $
    memcpy newBytes (castPtr str) (len + 1)
  return (castPtr newBytes)

data PGresult_data

curOffset :: Ptr PGresult -> IO CInt
curOffset = #{peek struct pg_result, curOffset}

setCurOffset :: Ptr PGresult -> CInt -> IO ()
setCurOffset = #{poke struct pg_result, curOffset}

incCurOffset :: Ptr PGresult -> CInt -> IO ()
incCurOffset res amt = do
  old <- curOffset res
  setCurOffset res (old + amt)

spaceLeft :: Ptr PGresult -> IO CInt
spaceLeft = #{peek struct pg_result, spaceLeft}

setSpaceLeft :: Ptr PGresult -> CInt -> IO ()
setSpaceLeft = #{poke struct pg_result, spaceLeft}

decSpaceLeft :: Ptr PGresult -> CInt -> IO ()
decSpaceLeft res amt = do
  old <- spaceLeft res
  setSpaceLeft res (old - amt)

curBlock :: Ptr PGresult -> IO (Ptr PGresult_data)
curBlock = #{peek struct pg_result, curBlock}

setCurBlock :: Ptr PGresult -> Ptr PGresult_data -> IO ()
setCurBlock = #{poke struct pg_result, curBlock}

space :: Ptr PGresult_data -> CSize -> Ptr Word8
space block offset =
  let ptr = #{ptr PGresult_data, space} block
  in plusPtr ptr (fromIntegral offset)

next :: Ptr PGresult_data -> IO (Ptr PGresult_data)
next = #{peek PGresult_data, next}

setNext :: Ptr PGresult_data -> Ptr PGresult_data -> IO ()
setNext = #{poke PGresult_data, next}

resultAlloc :: Ptr PGresult -> CSize -> Bool -> IO (Ptr Word8)
resultAlloc res amt align =
  if (amt <= 0)
  then return $ #{ptr struct pg_result, null_field} res
  else do
    when align $ do
      offset <- (`mod` pGRESULT_ALIGN_BOUNDARY) `fmap` curOffset res
      when (offset /= 0) $ do
        incCurOffset res (pGRESULT_ALIGN_BOUNDARY - offset)
        decSpaceLeft res (pGRESULT_ALIGN_BOUNDARY - offset)
    oldSpaceLeft <- spaceLeft res
    if amt <= fromIntegral oldSpaceLeft
       then storeInSpace res amt
       else if amt >= pGRESULT_SEP_ALLOC_THRESHOLD
               then storeInOwnSpace res amt
               else storeInNewSpace res amt align

pGRESULT_DATA_BLOCKSIZE, pGRESULT_ALIGN_BOUNDARY, pGRESULT_BLOCK_OVERHEAD, pGRESULT_SEP_ALLOC_THRESHOLD :: (Integral a) => a
pGRESULT_DATA_BLOCKSIZE = 2048
pGRESULT_ALIGN_BOUNDARY = #{const MAXIMUM_ALIGNOF}
pGRESULT_BLOCK_OVERHEAD = max #{size PGresult_data} pGRESULT_ALIGN_BOUNDARY
pGRESULT_SEP_ALLOC_THRESHOLD = pGRESULT_DATA_BLOCKSIZE `div` 2

-- current block has enough room and offset is properly aligned
storeInSpace :: Ptr PGresult -> CSize -> IO (Ptr Word8)
storeInSpace res amt = do
  block <- curBlock res
  offs <- curOffset res
  let result = space block (fromIntegral offs)
  incCurOffset res (fromIntegral amt)
  decSpaceLeft res (fromIntegral amt)
  return result

-- Amt is too large; put it in its own block, *behind* the current block, if
-- there is one
storeInOwnSpace :: Ptr PGresult -> CSize -> IO (Ptr Word8)
storeInOwnSpace res amt = do
  block <- mallocBytes (fromIntegral $ amt + pGRESULT_BLOCK_OVERHEAD)
  if block == nullPtr
     then return nullPtr
     else do
       let result = space block pGRESULT_BLOCK_OVERHEAD
       oldCurBlock <- curBlock res
       if oldCurBlock == nullPtr
          then do
            setNext block nullPtr
            setCurBlock res block
            setSpaceLeft res 0
          else do
            oldNext <- next oldCurBlock
            setNext block oldNext
            setNext oldCurBlock block
       return result

storeInNewSpace :: Ptr PGresult -> CSize -> Bool -> IO (Ptr Word8)
storeInNewSpace res amt align = do
  block <- mallocBytes pGRESULT_DATA_BLOCKSIZE
  if block == nullPtr
   then return nullPtr
   else do
     oldCurBlock <- curBlock res
     setNext block oldCurBlock
     setCurBlock res block
     if align
        then do
          setCurOffset res pGRESULT_BLOCK_OVERHEAD
          setSpaceLeft res (pGRESULT_DATA_BLOCKSIZE - pGRESULT_BLOCK_OVERHEAD)
        else do
          setCurOffset res #{size PGresult_data}
          setSpaceLeft res (pGRESULT_DATA_BLOCKSIZE - #{size PGresult_data})
     newCurOffset <- curOffset res
     let result = space block (fromIntegral newCurOffset)
     incCurOffset res (fromIntegral amt)
     decSpaceLeft res (fromIntegral amt)
     return result

{- setResultError isn't actually linkable.  It's also not
   trivial to reproduce -- the whole PGresult subsidiary storage
   mechanism is unexported.

foreign import ccall unsafe "libpq-int.h pqSetResultError"
    c_setResultError :: Ptr PGresult -> Ptr CChar -> IO ()
-}
