{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses, TypeFamilies, OverloadedStrings #-}

module Aws.DynamoDb.Core.Conduit
  ( ConduitResponse
  ) where

import Data.Conduit (ConduitM)

import Control.Monad.Trans.Resource (ResourceT)

import Aws.Core
import Aws.DynamoDb.Core
import Aws.DynamoDb.Commands.Query

type ConduitResponse a r = ConduitM () a (ResourceT IO) r
