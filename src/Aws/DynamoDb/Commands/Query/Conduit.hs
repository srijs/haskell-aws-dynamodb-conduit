{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, TypeFamilies, OverloadedStrings, RecordWildCards #-}

module Aws.DynamoDb.Commands.Query.Conduit
  ( module Aws.DynamoDb.Commands.Query
  , ConduitQuery(..), ConduitQueryResponse
  ) where

import Data.Aeson.Types (parseMaybe)
import Data.Conduit (ConduitM, fuse, yield, await, unwrapResumable)
import Data.JSON.ToGo.Parser
import Data.Maybe (fromMaybe)
import Data.Monoid (Monoid, mempty)
import Data.Sequence (Seq, singleton)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Parser (ParserT(..), ResultM(..), runStateParserT)
import Control.Monad.Trans.Resource (register, runResourceT, throwM, transResourceT)
import Control.Monad.Trans.State (StateT, modify)
import Control.Monad.Trans.Writer (WriterT, tell, runWriterT)

import qualified Network.HTTP.Types as HTTP
import qualified Network.HTTP.Conduit as HTTP

import Aws.Core
import Aws.DynamoDb.Core hiding (runParser)
import Aws.DynamoDb.Core.Conduit
import Aws.DynamoDb.Commands.Query

data ConduitQuery = ConduitQuery { getQuery :: Query }

instance SignQuery ConduitQuery where
  type ServiceConfiguration ConduitQuery = DdbConfiguration
  signQuery = ddbSignQuery "Query" . getQuery

data IncompleteQueryResponse = IncompleteQueryResponse
  { incompleteLastKey  :: Maybe [Attribute]
  , incompleteCount    :: Maybe Int
  , incompleteScanned  :: Maybe Int
  , incompleteConsumed :: Maybe ConsumedCapacity
  }

defaultIncompleteQueryResponse = IncompleteQueryResponse Nothing Nothing Nothing Nothing

toQueryResponse :: IncompleteQueryResponse -> Maybe (QueryResponse)
toQueryResponse icr = do
  let items    = mempty
      lastKey  = incompleteLastKey  icr
      consumed = incompleteConsumed icr
  count   <- incompleteCount   icr
  scanned <- incompleteScanned icr
  return $ QueryResponse items lastKey count scanned consumed

responseParser :: Monad m => ParserM (StateT IncompleteQueryResponse (WriterT (Seq Item) m)) ()
responseParser = pobject key
  where 
    key "Count"            = parse  >>= lift . modify . setCount    . Just
    key "ScannedCount"     = parse  >>= lift . modify . setScanned  . Just
    key "ConsumedCapacity" = parse  >>= lift . modify . setConsumed . Just
    key "LastKey"          = pvalue >>= lift . modify . setLastKey  . parseMaybe parseAttributeJson
    key "Items"            = parray . const $ parse >>= lift . lift . tell . maybeToSeq
    key _                  = pvalue >> return ()
    setCount    c icq = icq { incompleteCount    = c }
    setScanned  c icq = icq { incompleteScanned  = c }
    setConsumed c icq = icq { incompleteConsumed = c }
    setLastKey  c icq = icq { incompleteLastKey  = c }
    maybeToSeq = maybe mempty singleton

conduitWriterParserT :: (Monad m, Monoid i, Eq i) => ParserT i (WriterT a m) r -> ConduitM i a m r
conduitWriterParserT p = await' >>= lift . runWriterT . runParserT p >>= \w -> case w of
  (PartialM p', a) -> yield a >> conduitWriterParserT p'
  (FailM i s,   a) -> yield a >> fail s
  (DoneM i r,   a) -> yield a >> return r
  where await' = await >>= maybe (return mempty) (\i -> if i == mempty then await' else return i)

consume p rsrc = do
  src <- lift $ do
    (src, finalize) <- unwrapResumable rsrc
    finalize' <- transResourceT return finalize
    register finalize'
    return src
  fuse src $ conduitWriterParserT p

instance Transaction ConduitQuery ConduitQueryResponse

type ConduitQueryResponse = ConduitResponse (Seq Item) QueryResponse

instance ResponseConsumer ConduitQuery ConduitQueryResponse where
  type ResponseMetadata ConduitQueryResponse = DdbResponse
  responseConsumer _ ref resp = do
    tellMeta
    return $ case statusCode of
      200 -> rSuccess
      _   -> rError
    where
      header = fmap T.decodeUtf8 . flip lookup (HTTP.responseHeaders resp)
      amzId = header "x-amzn-RequestId"
      amzCrc = header "x-amz-crc32"
      meta = DdbResponse amzCrc amzId
      tellMeta = liftIO $ tellMetadataRef ref meta

      rSuccess = do
        iqr <- consume (runStateParserT responseParser defaultIncompleteQueryResponse) $ HTTP.responseBody resp
        maybe (fail "incomplete") return (toQueryResponse iqr)

      rError = do
        err'' <- consume parse $ HTTP.responseBody resp
        errCode <- readErrCode . T.drop 1 . snd . T.breakOn "#" $ aeType err''
        throwM . DdbError statusCode errCode . fromMaybe "" $ aeMessage err''

      readErrCode = return . read . T.unpack

      HTTP.Status{..} = HTTP.responseStatus resp
