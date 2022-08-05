package writer

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/ledger/ledgercore"
	"github.com/algorand/go-algorand/protocol"
	"github.com/algorand/indexer/idb"
	"github.com/algorand/indexer/idb/postgres/internal/encoding"
	"github.com/algorand/indexer/idb/postgres/internal/schema"
	"github.com/jackc/pgx/v4"
)

const (
	setSpecialAccountsStmtName         = "set_special_accounts"
	upsertAssetStmtName                = "upsert_asset"
	upsertAccountAssetStmtName         = "upsert_account_asset"
	upsertAppStmtName                  = "upsert_app"
	upsertAccountAppStmtName           = "upsert_account_app"
	deleteAccountStmtName              = "delete_account"
	deleteAccountUpdateKeytypeStmtName = "delete_account_update_keytype"
	upsertAccountStmtName              = "upsert_account"
	upsertAccountWithKeytypeStmtName   = "upsert_account_with_keytype"
	deleteAssetStmtName                = "delete_asset"
	deleteAccountAssetStmtName         = "delete_account_asset"
	deleteAppStmtName                  = "delete_app"
	deleteAccountAppStmtName           = "delete_account_app"
	updateAccountTotalsStmtName        = "update_account_totals"
)

const (
	DAOName      = "dao_name"
	GovTokenId   = "gov_token_id"
	SigmaDAOHash = "BiAFAQAEAgMmGQp2b3RpbmdfZW5kA3llcwdkZXBvc2l0DmV4ZWN1dGVfYmVmb3JlAm5vBHR5cGULbWluX3N1cHBvcnQCaWQMZ292X3Rva2VuX2lkCGV4ZWN1dGVkBGZyb20JcmVjaXBpZW50BmFtb3VudAJwXwdhYnN0YWluA3VybAloYXNoX2FsZ28Mdm90aW5nX3N0YXJ0BmFzYV9pZAxkZXBvc2l0X2xvY2sEbmFtZQh1cmxfaGFzaANtc2cMbWluX2R1cmF0aW9uDG1heF9kdXJhdGlvbjEYIxJABlExGSQSMRmBBRIRQAZCMRklEjEZIhIRQAYvNhoAgA9vcHRpbl9nb3ZfdG9rZW4SQAXuNhoAgAxhZGRfcHJvcG9zYWwSQASaNhoAgBJkZXBvc2l0X3ZvdGVfdG9rZW4SQARMNhoAgA1yZWdpc3Rlcl92b3RlEkADgjYaAIAHZXhlY3V0ZRJAAmw2GgCAFXdpdGhkcmF3X3ZvdGVfZGVwb3NpdBJAAgk2GgCAEWNsZWFyX3ZvdGVfcmVjb3JkEkABJjYaAIAOY2xvc2VfcHJvcG9zYWwSQAABADIHIytiDUAA/zIHIyhiDkAA7zIHIyhiDSMpYicGZA8QIyliIycEYg0QIhJAAM0yByMrYg4yByMoYg0QMgcjKGINIyliJwZkDxAjKWIjJwRiDRAjEhBAAAEAJTUAIyMnB2M1CzUKMgQiEjQLIhIQMSAyAxIxCTIDEhAxFTIDEhAQIycJYiISIytiMgcMESISNAAiEyMoYjIHDBAiEhEQRLEkshAnCGSyETEAshQqZLISI7IBsyMnFGgjJw9oIycVaCMnEGgjJxFoIyhoIytoIycFaCMnCmgjJwtoIycSaCMnDGgjJxZoIycHaCMnCWgjKWgjJwRoIycOaCJDIjUAQv9bIQQ1AEL/VCQ1AEL/TiMjJw02HAFQYzUJNQgyByIrYg1AAK0yByIoYg5AAJ0yByIoYg0iKWInBmQPECIpYiInBGINECISQAB7MgciK2IOMgciKGINEDIHIihiDSIpYicGZA8QIiliIicEYg0QIxIQQAABACU1ADIHIihiDjIHIitiDjQAIhIQIicJYiMSEBFAACslNQEyBCISRDQJIhJAAAojJw02HAFQaCJDNAgiJwdiEjQBIhIQIhJB/+UAIjUBQv/SIjUAQv+tIQQ1AEL/piQ1AEL/oDIEIhIxIDIDEjEJMgMSEDEVMgMSEBAyBzEAJxNiDRBEsSSyECcIZLIRMQCyFDYaAReyEiOyAbMxACojKmI2GgEXCWYiQzIHIitiDUAA9zIHIihiDkAA5zIHIihiDSIpYicGZA8QIiliIicEYg0QIhJAAMUyByIrYg4yByIoYg0QMgciKGINIiliJwZkDxAiKWIiJwRiDRAjEhBAAAEAJTUANAAiEiInCWIjEhBEIicFYiISQABZIicFYiUSQAAXIicFYiEEEkAAAQAyBCISRCInCSJmIkMyBCUSMwEQJBIQMwETMgMSEDMBACInCmISEDMBFCInC2ISEDMBEiInDGISEDMBESInEmISEERC/8AyBCUSMwEQIhIQMwEAIicKYhIQMwEHIicLYhIQMwEIIicMYhIQREL/lyI1AEL/YyEENQBC/1wkNQBC/1YjIycNNhwBUGM1CTUIMgQiEiInEWIyBw4QMgciKGIOECMqYiMNEEQ0CSMSQAB3NAgiJwdiE0AAXgA3ABoBKRJAAEc3ABoBJwQSQAAuNwAaAScOEkAAAQAiJw4iJw5iIypiCGYjJxNiIihiDkAAAiJDIycTIihiZkL/9CInBCInBGIjKmIIZkL/2iIpIiliIypiCGZC/80jJw02HAFQIicHYmZC/5QjJw02HAFQIicHYmZC/4UyBCINIjggMgMSECI4ECQSECI4EScIZBIQIjgUMgoSECI4EiMPEEQjKiMqYjMBEghmIkMjJwViIxJEMgQiDSI4IDIDEhAiOBAkEhAiOBEnCGQSECI4FDIKEhAiOBIjDxBEMwEgMgMSMwESKmQSEEQjJxQ2GgFmIycPNhoCZiMnFTYaA2Y2GgSAABJAANYjJxA2GgRmNhoFFzUCNhoGFzUDNhoHFzUENhoIFzUFNAIyBw1EIycRNAJmNAM0Ag0nF2Q0AzQCCQ4QJxhkNAM0AgkPEEQjKDQDZjQENAMNRCMrNARmNAUiEjQFJRIRNAUhBBIRRCMnBTQFZiMnBWIiEkAASSMnBWIlEkAAHyMnBWIhBBJAAAEAIycWNhoJZiMnBzEXZiMnCSNmIkMjJwo2GglmIycSNhoKF2YjJws2GgtmIycMNhoMF2ZC/9IjJwo2GglmIycLNhoKZiMnDDYaCxdmQv+5IycQgAZzaGEyNTZmQv8iMgQiEjEgMgMSEDYwACcIZBIQRLEkshAnCGSyETIKshQjshIjsgGzIkMyBCISRCJDI0M2GgIXIw02GgIXNhoDFwwQNhoGF3EANQc1BjQHEEQqNhoAF2cnBjYaARdnJxc2GgIXZycYNhoDF2cnDzYaBGeACGRhb19uYW1lNhoFZycINhoGF2ciQw=="
)

var statements = map[string]string{
	setSpecialAccountsStmtName: `INSERT INTO metastate (k, v) VALUES ('` +
		schema.SpecialAccountsMetastateKey +
		`', $1) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v`,
	upsertAssetStmtName: `INSERT INTO asset
		(index, creator_addr, params, deleted)
		VALUES($1, $2, $3, FALSE) ON CONFLICT (index) DO UPDATE SET
		creator_addr = EXCLUDED.creator_addr, params = EXCLUDED.params, deleted = FALSE`,
	upsertAccountAssetStmtName: `INSERT INTO account_asset
		(addr, assetid, amount, frozen, deleted)
		VALUES($1, $2, $3, $4, FALSE) ON CONFLICT (addr, assetid) DO UPDATE SET
		amount = EXCLUDED.amount, frozen = EXCLUDED.frozen, deleted = FALSE`,
	upsertAppStmtName: `INSERT INTO app
		(index, creator, params, dao_name, asset_id, deleted)
		VALUES($1, $2, $3, $4, $5, FALSE) ON CONFLICT (index) DO UPDATE SET
		creator = EXCLUDED.creator, params = EXCLUDED.params, dao_name = EXCLUDED.dao_name,
		asset_id = EXCLUDED.asset_id, deleted = FALSE`,
	upsertAccountAppStmtName: `INSERT INTO account_app
		(addr, app, localstate, deleted)
		VALUES($1, $2, $3, FALSE) ON CONFLICT (addr, app) DO UPDATE SET
		localstate = EXCLUDED.localstate, deleted = FALSE`,
	deleteAccountStmtName: `INSERT INTO account
		(addr, microalgos, rewardsbase, rewards_total, deleted, account_data)
		VALUES($1, 0, 0, 0, TRUE, 'null'::jsonb) ON CONFLICT (addr) DO UPDATE SET
		microalgos = EXCLUDED.microalgos, rewardsbase = EXCLUDED.rewardsbase,
		rewards_total = EXCLUDED.rewards_total, deleted = TRUE,
		account_data = EXCLUDED.account_data`,
	deleteAccountUpdateKeytypeStmtName: `INSERT INTO account
		(addr, microalgos, rewardsbase, rewards_total, deleted, keytype, account_data)
		VALUES($1, 0, 0, 0, TRUE, $2, 'null'::jsonb) ON CONFLICT (addr) DO UPDATE SET
		microalgos = EXCLUDED.microalgos, rewardsbase = EXCLUDED.rewardsbase,
		rewards_total = EXCLUDED.rewards_total, deleted = TRUE,
		keytype = EXCLUDED.keytype, account_data = EXCLUDED.account_data`,
	upsertAccountStmtName: `INSERT INTO account
		(addr, microalgos, rewardsbase, rewards_total, deleted, account_data)
		VALUES($1, $2, $3, $4, FALSE, $5) ON CONFLICT (addr) DO UPDATE SET
		microalgos = EXCLUDED.microalgos, rewardsbase = EXCLUDED.rewardsbase,
		rewards_total = EXCLUDED.rewards_total, deleted = FALSE,
		account_data = EXCLUDED.account_data`,
	upsertAccountWithKeytypeStmtName: `INSERT INTO account
		(addr, microalgos, rewardsbase, rewards_total, deleted, keytype, account_data)
		VALUES($1, $2, $3, $4, FALSE, $5, $6) ON CONFLICT (addr) DO UPDATE SET
		microalgos = EXCLUDED.microalgos, rewardsbase = EXCLUDED.rewardsbase,
		rewards_total = EXCLUDED.rewards_total, deleted = FALSE, keytype = EXCLUDED.keytype,
		account_data = EXCLUDED.account_data`,
	deleteAssetStmtName: `INSERT INTO asset
		(index, creator_addr, params, deleted)
		VALUES($1, $2, 'null'::jsonb, TRUE) ON CONFLICT (index) DO UPDATE SET
		creator_addr = EXCLUDED.creator_addr, params = EXCLUDED.params, deleted = TRUE`,
	deleteAccountAssetStmtName: `INSERT INTO account_asset
		(addr, assetid, amount, frozen, deleted)
		VALUES($1, $2, 0, false, TRUE) ON CONFLICT (addr, assetid) DO UPDATE SET
		amount = EXCLUDED.amount, frozen = TRUE, deleted = TRUE`,
	deleteAppStmtName: `INSERT INTO app
		(index, creator, params, dao_name, asset_id, deleted)
		VALUES($1, $2, 'null'::jsonb, $3, $4, TRUE) ON CONFLICT (index) DO UPDATE SET
		creator = EXCLUDED.creator, params = EXCLUDED.params, dao_name = EXCLUDED.dao_name,
		asset_id = EXCLUDED.asset_id, deleted = TRUE`,
	deleteAccountAppStmtName: `INSERT INTO account_app
		(addr, app, localstate, deleted)
		VALUES($1, $2, 'null'::jsonb, TRUE) ON CONFLICT (addr, app) DO UPDATE SET
		localstate = EXCLUDED.localstate, deleted = TRUE`,
	updateAccountTotalsStmtName: `UPDATE metastate SET v = $1 WHERE k = '` +
		schema.AccountTotals + `'`,
}

// Writer is responsible for writing blocks and accounting state deltas to the database.
type Writer struct {
	tx pgx.Tx
}

// MakeWriter creates a Writer object.
func MakeWriter(tx pgx.Tx) (Writer, error) {
	w := Writer{
		tx: tx,
	}

	for name, query := range statements {
		_, err := tx.Prepare(context.Background(), name, query)
		if err != nil {
			return Writer{}, fmt.Errorf("MakeWriter() prepare statement err: %w", err)
		}
	}

	return w, nil
}

// Close shuts down Writer.
func (w *Writer) Close() {
	for name := range statements {
		w.tx.Conn().Deallocate(context.Background(), name)
	}
}

func setSpecialAccounts(addresses transactions.SpecialAddresses, batch *pgx.Batch) {
	j := encoding.EncodeSpecialAddresses(addresses)
	batch.Queue(setSpecialAccountsStmtName, j)
}

// Describes a change to the `account.keytype` column. If `present` is true,
// `value` is the new value. Otherwise, NULL will be the new value.
type sigTypeDelta struct {
	present bool
	value   idb.SigType
}

func getSigTypeDeltas(payset []transactions.SignedTxnInBlock) (map[basics.Address]sigTypeDelta, error) {
	res := make(map[basics.Address]sigTypeDelta, len(payset))

	for i := range payset {
		if payset[i].Txn.RekeyTo == (basics.Address{}) && payset[i].Txn.Type != protocol.StateProofTx {
			sigtype, err := idb.SignatureType(&payset[i].SignedTxn)
			if err != nil {
				return nil, fmt.Errorf("getSigTypeDelta() err: %w", err)
			}
			res[payset[i].Txn.Sender] = sigTypeDelta{present: true, value: sigtype}
		} else {
			res[payset[i].Txn.Sender] = sigTypeDelta{}
		}
	}

	return res, nil
}

type optionalSigTypeDelta struct {
	present bool
	value   sigTypeDelta
}

func writeAccount(round basics.Round, address basics.Address, accountData ledgercore.AccountData, sigtypeDelta optionalSigTypeDelta, batch *pgx.Batch) {
	sigtypeFunc := func(delta sigTypeDelta) *idb.SigType {
		if !delta.present {
			return nil
		}

		res := new(idb.SigType)
		*res = delta.value
		return res
	}

	if accountData.IsZero() {
		// Delete account.
		if sigtypeDelta.present {
			batch.Queue(
				deleteAccountUpdateKeytypeStmtName,
				address[:], sigtypeFunc(sigtypeDelta.value))
		} else {
			batch.Queue(deleteAccountStmtName, address[:])
		}
	} else {
		// Update account.
		accountDataJSON :=
			encoding.EncodeTrimmedLcAccountData(encoding.TrimLcAccountData(accountData))

		if sigtypeDelta.present {
			batch.Queue(
				upsertAccountWithKeytypeStmtName,
				address[:], accountData.MicroAlgos.Raw, accountData.RewardsBase,
				accountData.RewardedMicroAlgos.Raw, sigtypeFunc(sigtypeDelta.value),
				accountDataJSON)
		} else {
			batch.Queue(
				upsertAccountStmtName,
				address[:], accountData.MicroAlgos.Raw, accountData.RewardsBase,
				accountData.RewardedMicroAlgos.Raw, accountDataJSON)
		}
	}
}

func writeAssetResource(round basics.Round, resource *ledgercore.AssetResourceRecord, batch *pgx.Batch) {
	if resource.Params.Deleted {
		batch.Queue(deleteAssetStmtName, resource.Aidx, resource.Addr[:])
	} else {
		if resource.Params.Params != nil {
			batch.Queue(
				upsertAssetStmtName, resource.Aidx, resource.Addr[:],
				encoding.EncodeAssetParams(*resource.Params.Params))
		}
	}

	if resource.Holding.Deleted {
		batch.Queue(deleteAccountAssetStmtName, resource.Addr[:], resource.Aidx)
	} else {
		if resource.Holding.Holding != nil {
			batch.Queue(
				upsertAccountAssetStmtName, resource.Addr[:], resource.Aidx,
				strconv.FormatUint(resource.Holding.Holding.Amount, 10),
				resource.Holding.Holding.Frozen)
		}
	}
}

func writeAppResource(round basics.Round, resource *ledgercore.AppResourceRecord, batch *pgx.Batch) {
	if resource.Params.Params != nil && resource.Params.Params.ApprovalProgram != nil {
		b64 := base64.StdEncoding
		var appHash = b64.EncodeToString([]byte(resource.Params.Params.ApprovalProgram))
		// allow only SigmaDAO app
		if SigmaDAOHash == appHash {
			daoName := resource.Params.Params.GlobalState[DAOName]
			assetId := resource.Params.Params.GlobalState[GovTokenId]
			if resource.Params.Deleted {
				batch.Queue(deleteAppStmtName, resource.Aidx, resource.Addr[:], daoName.Bytes, assetId.Uint)
			} else {
				if resource.Params.Params != nil {
					batch.Queue(
						upsertAppStmtName, resource.Aidx, resource.Addr[:],
						encoding.EncodeAppParams(*resource.Params.Params), daoName.Bytes, assetId.Uint)
				}
			}
		}
	}

	if resource.State.Deleted {
		batch.Queue(deleteAccountAppStmtName, resource.Addr[:], resource.Aidx)
	} else {
		if resource.State.LocalState != nil {
			batch.Queue(
				upsertAccountAppStmtName, resource.Addr[:], resource.Aidx,
				encoding.EncodeAppLocalState(*resource.State.LocalState))
		}
	}
}

func writeAccountDeltas(round basics.Round, accountDeltas *ledgercore.AccountDeltas, sigtypeDeltas map[basics.Address]sigTypeDelta, batch *pgx.Batch) {
	// Update `account` table.
	for i := 0; i < accountDeltas.Len(); i++ {
		address, accountData := accountDeltas.GetByIdx(i)

		var sigtypeDelta optionalSigTypeDelta
		sigtypeDelta.value, sigtypeDelta.present = sigtypeDeltas[address]

		writeAccount(round, address, accountData, sigtypeDelta, batch)
	}

	// Update `asset` and `account_asset` tables.
	{
		assetResources := accountDeltas.GetAllAssetResources()
		for i := range assetResources {
			writeAssetResource(round, &assetResources[i], batch)
		}
	}

	// Update `app` and `account_app` tables.
	{
		appResources := accountDeltas.GetAllAppResources()
		for i := range appResources {
			writeAppResource(round, &appResources[i], batch)
		}
	}
}

// AddBlock0 writes block 0 to the database.
func (w *Writer) AddBlock0(block *bookkeeping.Block) error {
	var batch pgx.Batch

	specialAddresses := transactions.SpecialAddresses{
		FeeSink:     block.FeeSink,
		RewardsPool: block.RewardsPool,
	}
	setSpecialAccounts(specialAddresses, &batch)

	results := w.tx.SendBatch(context.Background(), &batch)
	// Clean the results off the connection's queue. Without this, weird things happen.
	for i := 0; i < batch.Len(); i++ {
		_, err := results.Exec()
		if err != nil {
			results.Close()
			return fmt.Errorf("AddBlock() exec err: %w", err)
		}
	}
	err := results.Close()
	if err != nil {
		return fmt.Errorf("AddBlock() close results err: %w", err)
	}

	return nil
}

// AddBlock writes the block and accounting state deltas to the database, except for
// transactions and transaction participation. Those are imported by free functions in
// the writer/ directory.
func (w *Writer) AddBlock(block *bookkeeping.Block, modifiedTxns []transactions.SignedTxnInBlock, delta ledgercore.StateDelta) error {
	var batch pgx.Batch

	specialAddresses := transactions.SpecialAddresses{
		FeeSink:     block.FeeSink,
		RewardsPool: block.RewardsPool,
	}
	setSpecialAccounts(specialAddresses, &batch)
	{
		sigTypeDeltas, err := getSigTypeDeltas(block.Payset)
		if err != nil {
			return fmt.Errorf("AddBlock() err: %w", err)
		}
		writeAccountDeltas(block.Round(), &delta.Accts, sigTypeDeltas, &batch)
	}
	batch.Queue(updateAccountTotalsStmtName, encoding.EncodeAccountTotals(&delta.Totals))

	results := w.tx.SendBatch(context.Background(), &batch)
	// Clean the results off the connection's queue. Without this, weird things happen.
	for i := 0; i < batch.Len(); i++ {
		_, err := results.Exec()
		if err != nil {
			results.Close()
			return fmt.Errorf("AddBlock() exec err: %w", err)
		}
	}
	err := results.Close()
	if err != nil {
		return fmt.Errorf("AddBlock() close results err: %w", err)
	}

	return nil
}
