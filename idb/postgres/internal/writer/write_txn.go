package writer

import (
	"fmt"

	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/protocol"
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
