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
import Data.ByteString as B
import Data.ByteString.Unsafe as B

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

setResultError :: Result -> B.ByteString -> IO ()
setResultError r e =
  withResult r $ \rPtr ->
    B.unsafeUseAsCString e $ \ePtr ->
      error "TODO" -- c_setResultError rPtr ePtr

{- setResultError isn't actually linkable.  It's also not
   trivial to reproduce -- the whole PGresult subsidiary storage
   mechanism is unexported.

foreign import ccall unsafe "libpq-int.h pqSetResultError"
    c_setResultError :: Ptr PGresult -> Ptr CChar -> IO ()
-}
