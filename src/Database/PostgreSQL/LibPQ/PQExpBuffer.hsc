{-# LANGUAGE ForeignFunctionInterface   #-}
{-# LANGUAGE EmptyDataDecls             #-}

module Database.PostgreSQL.LibPQ.PQExpBuffer
    (resetPQExpBuffer
    ,replacePQExpBuffer
    ,appendPQExpBuffer
    ,pqExpBufferToByteString
    ,PQExpBuffer
    ,PQExpBufferData
    )
where

import Foreign
import Foreign.C.Types
import qualified Data.ByteString.Internal as B

#include <internal/c.h>
#include <internal/pqexpbuffer.h>

data PQExpBufferData
type PQExpBuffer = Ptr PQExpBufferData

pqExpBufferToByteString :: PQExpBuffer -> IO B.ByteString
pqExpBufferToByteString strPtr = do
  len <- #{peek PQExpBufferData, len} strPtr :: IO CSize
  B.create (fromIntegral len) $ \resultPtr -> do
    strPtrRaw <- #{peek PQExpBufferData, data} strPtr :: IO (Ptr Word8)
    B.memcpy resultPtr strPtrRaw len

replacePQExpBuffer :: PQExpBuffer -> B.ByteString -> IO ()
replacePQExpBuffer str text = do
  resetPQExpBuffer str
  appendPQExpBuffer str text

resetPQExpBuffer :: PQExpBuffer -> IO ()
resetPQExpBuffer = c_resetPQExpBuffer

appendPQExpBuffer :: PQExpBuffer -> B.ByteString -> IO ()
appendPQExpBuffer strPtr text =
  let (fp, off, len) = B.toForeignPtr text
  in withForeignPtr fp $ \textPtr ->
    c_appendBinaryPQExpBuffer strPtr (plusPtr textPtr off) (fromIntegral len)

foreign import ccall unsafe "pqexpbuffer.h resetPQExpBuffer"
    c_resetPQExpBuffer :: PQExpBuffer -> IO ()

foreign import ccall unsafe "pqexpbuffer.h appendBinaryPQExpBuffer"
    c_appendBinaryPQExpBuffer :: PQExpBuffer -> Ptr Word8 -> CSize -> IO ()
