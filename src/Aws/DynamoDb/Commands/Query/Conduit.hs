{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, MultiParamTypeClasses, TypeFamilies, OverloadedStrings, RecordWildCards #-}

module Aws.DynamoDb.Commands.Query.Conduit
  ( module Aws.DynamoDb.Commands.Query
  , ConduitQuery(..), ConduitQueryResponse
  ) where

import Data.Aeson (parseJSON)
import Data.Aeson.Types (parseMaybe)
import Data.ByteString (ByteString)
import Data.Conduit (ConduitM, fuse, yield, await, ResumableSource, unwrapResumable)
import Data.IORef (IORef)
import Data.JSON.ToGo.Parser
import Data.Maybe (maybeToList, fromMaybe)
import Data.Monoid (Monoid, mempty, mappend, (<>), Last(..))
import Data.Sequence (Seq, singleton)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import Control.Applicative ((<$), (<$>), (<*>))
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Parser (ParserT, runParserT, runParserTWith, eitherResultM, ResultM(..))
import Control.Monad.Trans.Resource (ResourceT, register, runResourceT, throwM)
import Control.Monad.Trans.Writer.Lazy (WriterT, tell, runWriterT)

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
  { incompleteLastKey  :: Last [Attribute]
  , incompleteCount    :: Last Int
  , incompleteScanned  :: Last Int
  , incompleteConsumed :: Last ConsumedCapacity
  }

toQueryResponse :: IncompleteQueryResponse -> Maybe (QueryResponse)
toQueryResponse icr = do
  let items    = mempty
      lastKey  = getLast $ incompleteLastKey  icr
      consumed = getLast $ incompleteConsumed icr
  count   <- getLast $ incompleteCount   icr
  scanned <- getLast $ incompleteScanned icr
  return $ QueryResponse items lastKey count scanned consumed

instance Monoid IncompleteQueryResponse where
  mempty = IncompleteQueryResponse mempty mempty mempty mempty
  mappend (IncompleteQueryResponse a b c d)
          (IncompleteQueryResponse e f g h)
    = IncompleteQueryResponse (a <> e) (b <> f) (c <> g) (d <> h)

responseParser :: Monad m => ParserM (WriterT (Seq Item) m) QueryResponse
responseParser = pobject key >>= maybe (fail "incomplete") return . toQueryResponse
  where 
    key "Count"            = setCount    <$> Just <$> parse
    key "ScannedCount"     = setScanned  <$> Just <$> parse
    key "ConsumedCapacity" = setConsumed <$> Just <$> parse
    key "LastKey"          = setLastKey  <$> parseMaybe parseAttributeJson <$> pvalue
    key "Items"            = mempty <$ (parray . const $ parse >>= lift . tell . maybeToSeq)
    key _                  = return mempty
    setCount    c = mempty { incompleteCount    = Last c }
    setScanned  c = mempty { incompleteScanned  = Last c }
    setConsumed c = mempty { incompleteConsumed = Last c }
    setLastKey  c = mempty { incompleteLastKey  = Last c }
    maybeToSeq = maybe mempty singleton

runParser :: (Monad m, Monoid i, Eq i) => ParserT i (WriterT a m) r -> ConduitM i a m r
runParser p = await' >>= lift . runWriterT . runParserT p >>= \w -> case w of
  (PartialM p', a) -> yield a >> runParser p'
  (FailM i s,   a) -> yield a >> fail s
  (DoneM i r,   a) -> yield a >> return r
  where await' = await >>= maybe (return mempty) (\i -> if i == mempty then await' else return i)

consume p rsrc = do
  src <- lift $ do
    (src, finalize) <- unwrapResumable rsrc
    register (runResourceT finalize)
    return src
  fuse src $ runParser p

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

      rSuccess = consume responseParser $ HTTP.responseBody resp

      rError = do
        err'' <- consume parse $ HTTP.responseBody resp
        errCode <- readErrCode . T.drop 1 . snd . T.breakOn "#" $ aeType err''
        throwM . DdbError statusCode errCode . fromMaybe "" $ aeMessage err''

      readErrCode = return . read . T.unpack

      HTTP.Status{..} = HTTP.responseStatus resp
