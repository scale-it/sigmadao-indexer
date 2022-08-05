package writer

import (
	"context"
	"fmt"

	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/protocol"
	"github.com/jackc/pgx/v4"

	"github.com/algorand/indexer/idb"
	"github.com/algorand/indexer/idb/postgres/internal/encoding"
)

// Get the ID of the creatable referenced in the given transaction
// (0 if not an asset or app transaction).
// Note: ConsensusParams.MaxInnerTransactions could be overridden to force
//       generating ApplyData.{ApplicationID/ConfigAsset}. This function does
//       other things too, so it is not clear we should use it. The only
//       real benefit is that it would slightly simplify this function by
//       allowing us to leave out the intra / block parameters.
func transactionAssetID(stxnad *transactions.SignedTxnWithAD, intra uint, block *bookkeeping.Block) (uint64, error) {
	assetid := uint64(0)

	switch stxnad.Txn.Type {
	case protocol.ApplicationCallTx:
		assetid = uint64(stxnad.Txn.ApplicationID)
		if assetid == 0 {
			assetid = uint64(stxnad.ApplyData.ApplicationID)
		}
		if assetid == 0 {
			if block == nil {
				return 0, fmt.Errorf("transactionAssetID(): Missing ApplicationID for transaction: %s", stxnad.ID())
			}
			// pre v30 transactions do not have ApplyData.ConfigAsset or InnerTxns
			// so txn counter + payset pos calculation is OK
			assetid = block.TxnCounter - uint64(len(block.Payset)) + uint64(intra) + 1
		}
	case protocol.AssetConfigTx:
		assetid = uint64(stxnad.Txn.ConfigAsset)
		if assetid == 0 {
			assetid = uint64(stxnad.ApplyData.ConfigAsset)
		}
		if assetid == 0 {
			if block == nil {
				return 0, fmt.Errorf("transactionAssetID(): Missing ConfigAsset for transaction: %s", stxnad.ID())
			}
			// pre v30 transactions do not have ApplyData.ApplicationID or InnerTxns
			// so txn counter + payset pos calculation is OK
			assetid = block.TxnCounter - uint64(len(block.Payset)) + uint64(intra) + 1
		}
	case protocol.AssetTransferTx:
		assetid = uint64(stxnad.Txn.XferAsset)
	case protocol.AssetFreezeTx:
		assetid = uint64(stxnad.Txn.FreezeAsset)
	}

	return assetid, nil
}

// Traverses the inner transaction tree and writes database rows
// to `outCh`. It performs a preorder traversal to correctly compute
// the intra round offset, the offset for the next transaction is returned.
func yieldInnerTransactions(ctx context.Context, stxnad *transactions.SignedTxnWithAD, block *bookkeeping.Block, intra, rootIntra uint, rootTxid string, outCh chan []interface{}) (uint, error) {
	for _, itxn := range stxnad.ApplyData.EvalDelta.InnerTxns {
		txn := &itxn.Txn
		typeenum, ok := idb.GetTypeEnum(txn.Type)
		if !ok {
			return 0, fmt.Errorf("yieldInnerTransactions() get type enum")
		}
		// block shouldn't be used for inner transactions.
		assetid, err := transactionAssetID(&itxn, 0, nil)
		if err != nil {
			return 0, err
		}
		extra := idb.TxnExtra{
			AssetCloseAmount: itxn.ApplyData.AssetClosingAmount,
			RootIntra:        idb.OptionalUint{Present: true, Value: rootIntra},
			RootTxid:         rootTxid,
		}

		// When encoding an inner transaction we remove any further nested inner transactions.
		// To reconstruct a full object the root transaction must be fetched.
		txnNoInner := itxn
		txnNoInner.EvalDelta.InnerTxns = nil
		row := []interface{}{
			uint64(block.Round()), intra, int(typeenum), assetid,
			nil, // inner transactions do not have a txid.
			encoding.EncodeSignedTxnWithAD(txnNoInner),
			encoding.EncodeTxnExtra(&extra)}
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("yieldInnerTransactions() ctx.Err(): %w", ctx.Err())
		case outCh <- row:
		}

		// Recurse at end for preorder traversal
		intra, err =
			yieldInnerTransactions(ctx, &itxn, block, intra+1, rootIntra, rootTxid, outCh)
		if err != nil {
			return 0, err
		}
	}

	return intra, nil
}

// Writes database rows for transactions (including inner transactions) to `outCh`.
func yieldTransactions(ctx context.Context, block *bookkeeping.Block, modifiedTxns []transactions.SignedTxnInBlock, outCh chan []interface{}) error {
	intra := uint(0)
	for idx, stib := range block.Payset {
		var stxnad transactions.SignedTxnWithAD
		var err error
		// This function makes sure to set correct genesis information so we can get the
		// correct transaction hash.
		stxnad.SignedTxn, stxnad.ApplyData, err = block.BlockHeader.DecodeSignedTxn(stib)
		if err != nil {
			return fmt.Errorf("yieldTransactions() decode signed txn err: %w", err)
		}

		txn := &stxnad.Txn
		typeenum, ok := idb.GetTypeEnum(txn.Type)
		if !ok {
			return fmt.Errorf("yieldTransactions() get type enum")
		}
		assetid, err := transactionAssetID(&stxnad, intra, block)
		if err != nil {
			return err
		}
		id := txn.ID().String()

		extra := idb.TxnExtra{
			AssetCloseAmount: modifiedTxns[idx].ApplyData.AssetClosingAmount,
		}
		row := []interface{}{
			uint64(block.Round()), intra, int(typeenum), assetid, id,
			encoding.EncodeSignedTxnWithAD(stxnad),
			encoding.EncodeTxnExtra(&extra)}
		select {
		case <-ctx.Done():
			return fmt.Errorf("yieldTransactions() ctx.Err(): %w", ctx.Err())
		case outCh <- row:
		}

		intra, err = yieldInnerTransactions(
			ctx, &stib.SignedTxnWithAD, block, intra+1, intra, id, outCh)
		if err != nil {
			return fmt.Errorf("yieldTransactions() adding inner: %w", err)
		}
	}

	return nil
}

// AddTransactions adds transactions from `block` to the database.
// `modifiedTxns` contains enhanced apply data generated by evaluator.
func AddTransactions(block *bookkeeping.Block, modifiedTxns []transactions.SignedTxnInBlock, tx pgx.Tx) error {
	return nil
}
