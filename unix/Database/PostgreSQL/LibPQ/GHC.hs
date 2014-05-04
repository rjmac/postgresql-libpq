------------------------------------------------------------------------------
-- |
-- Module:      Database.PostgreSQL.LibPQ.GHC
-- Copyright:   (c) 2013 Leon P Smith
-- License:     BSD3
-- Maintainer:  Leon P Smith <leon@melding-monads.com>
-- Stability:   experimental
--
-- On Unix,  this re-implements some of blocking libpq functions in terms of
-- on-blocking libpq operations and GHC's IO manager.   This allows
-- these operations to be interrupted by asynchronous exceptions,  and means
-- that GHC's runtime is responsible for handling the concurrency instead
-- of the OS kernel.
--
-- This module also re-exports the rest of libpq for convenience's sake.
-- Thus taking advantage of these features should be as simple as importing
-- @LibPQ.GHC@ instead of @LibPQ@.
--
-- On Windows,  this just re-exports the vanilla libpq bindings,  due to
-- the lack of a satisfactory IO manager on that platform.
--
------------------------------------------------------------------------------

{-# LANGUAGE OverloadedStrings #-}

module Database.PostgreSQL.LibPQ.GHC
    ( module Database.PostgreSQL.LibPQ
    , exec
    , connectdb
    , getResult
    , putCopyEnd
    ) where

import Database.PostgreSQL.LibPQ hiding (exec, connectdb, getResult, putCopyEnd)
import Database.PostgreSQL.LibPQ.Internal
import qualified Data.ByteString as B
import qualified Database.PostgreSQL.LibPQ as LibPQ
import Control.Concurrent (threadWaitRead, threadWaitWrite)
import System.Posix.Types (Fd)
import Control.Exception (bracket)
import Data.Maybe (fromMaybe)

ifM :: Monad m => m Bool -> m a -> m a -> m a
ifM c t e = do
  c' <- c
  if c'
     then t
     else e

withoutBlockingOn :: Connection -> IO a -> IO a
withoutBlockingOn conn act = ifM (isnonblocking conn) act wrappedAct
  where wrappedAct = bracket (setnonblocking conn True >> return ())
                             (\() -> setnonblocking conn False >> return ())
                             (\() -> act)

exec :: Connection -> B.ByteString -> IO (Maybe Result)
exec connection query =
  withoutBlockingOn connection $ ifM (execStart connection)
                                     (ifM (sendQuery connection query)
                                          (execFinish connection)
                                          (return Nothing))
                                     (return Nothing)

-- | A more or less literal translation of PQexecStart from libpq.
execStart :: Connection -> IO Bool
execStart connection = discardUnconsumedResults
  where
    -- An annoying little state machine.  Ultimately, what we're doing
    -- is consuming results until either something happens to break the
    -- connection or we've read and thrown away everything the server
    -- wants to send.  COPY statements in progress are also aborted (if
    -- possible).
    --
    -- The general flow is from discardUnconsumedResults to
    -- finishDiscardResult, possibly via an intermediary, and then
    -- back to discardUnconsumedResults until all results are gone.
    discardUnconsumedResults = do
      result <- getResult connection
      case result of
        Just r -> do
          rStat <- resultStatus r
          case rStat of
            CopyIn -> discard terminateCopyIn failBecauseOutstandingCopyIn
            CopyOut -> discard terminateCopyOut failBecauseOutstandingCopyOut
            -- CopyBoth -> discardCopyBoth -- Not yet bound in libpq
            _ -> finishDiscardResult
        Nothing -> return True
    discard terminate failure = do
      ifM isRecentPG terminate failure
    terminateCopyIn = do
      result <- putCopyEnd connection (Just "COPY terminated by new PQexec")
      case result of
        CopyInOk -> finishDiscardResult
        CopyInError -> return False
        CopyInWouldBlock -> error "Got CopyInWouldBlock in fake-blocking mode"
    terminateCopyOut = do
      setAsyncStatus connection AsyncBusy
      finishDiscardResult
    failBecauseOutstandingCopyIn = do
      setErrorMessage connection "COPY IN state must be terminated first\n" -- TODO: libpq_gettext
      return False
    failBecauseOutstandingCopyOut = do
      setErrorMessage connection "COPY OUT state must be terminated first\n" -- TODO: libpq_gettext
      return False
--    discardCopyBoth = do
--      setErrorMessage connection "PQexec not allowed during COPY BOTH\n" -- TODO: libpq_gettext
--      return False
    finishDiscardResult = do
      stat <- status connection
      if stat == ConnectionBad
        then return False
        else discardUnconsumedResults
    isRecentPG = do
      v <- protocolVersion connection
      return $ v >= 3

putCopyEnd :: Connection -> Maybe B.ByteString -> IO CopyInResult
putCopyEnd conn errmsg = withoutBlockingOn conn go
  where go = do
          result <- LibPQ.putCopyEnd conn errmsg
          case result of
            CopyInWouldBlock -> blockUntilWritable conn >> go
            other -> return other

execFinish :: Connection -> IO (Maybe Result)
execFinish connection = flushOutput connection >> awaitResult Nothing
  where awaitResult :: Maybe Result -> IO (Maybe Result)
        awaitResult lastResult = do
          result <- getResult connection
          case result of
            Just r ->
              -- More nasty C translation.  Subtlety here: if the previous result
              -- and this one are both FatalErrors, then it's the *previous* result
              -- that gets re-used.  I don't know if it matters, but it's what the
              -- C library does.
              case lastResult of
                Just lastR -> do
                  lastRStatus <- resultStatus lastR
                  rStatus <- resultStatus r
                  if lastRStatus == FatalError && rStatus == FatalError
                    then do
                      appendErrors lastR r
                      unsafeFreeResult r
                      finishHandlingResult lastR
                    else do
                      unsafeFreeResult lastR
                      finishHandlingResult r
                Nothing ->
                  finishHandlingResult r
            Nothing ->
              return lastResult
        appendErrors r1 r2 = do
          e1 <- fromMaybe "" `fmap` resultErrorMessage r1
          e2 <- fromMaybe "" `fmap` resultErrorMessage r2
          setResultError r1 (Just $ e1 `B.append` e2)
          e <- resultErrorMessage r1
          setErrorMessage connection (fromMaybe "" e)
        finishHandlingResult :: Result -> IO (Maybe Result)
        finishHandlingResult r =
          ifM (isEndStatus r)
              (return $ Just r)
              (ifM isBrokenConnection
                   (return $ Just r)
                   (awaitResult $ Just r))
        isEndStatus r = do
          rStatus <- resultStatus r
          return (rStatus == CopyIn || rStatus == CopyOut {- || rStatus == CopyBoth -})
        isBrokenConnection = do
          cStatus <- status connection
          return (cStatus == ConnectionBad)

flushOutput :: Connection -> IO ()
flushOutput connection = do
  flushStatus <- LibPQ.flush connection
  case flushStatus of
    FlushOk -> return ()
    FlushWriting -> blockUntilWritable connection >> flushOutput connection
    FlushFailed -> error "TODO" -- Not sure what to do here

getResult :: Connection -> IO (Maybe Result)
getResult connection = withoutBlockingOn connection go
  where go = ifM (consumeInput connection) checkResult noResult
        noResult =
          return Nothing
        checkResult =
          ifM (isBusy connection)
              (blockUntilReadable connection >> go)
              (LibPQ.getResult connection)

blockUntilReadable :: Connection -> IO ()
blockUntilReadable = blockUntil threadWaitRead

blockUntilWritable :: Connection -> IO ()
blockUntilWritable = blockUntil threadWaitWrite

blockUntil :: (Fd -> IO ()) -> Connection ->  IO ()
blockUntil test connection = do
  fd <- socket connection
  case fd of
    Just realfd -> test realfd
    Nothing -> error "block on a socketless connection?"

connectdb :: B.ByteString -> IO Connection
connectdb = LibPQ.connectdb
