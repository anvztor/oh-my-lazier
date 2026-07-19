package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/islishude/oh-my-lazier/go/internal/bigutil"
	"github.com/islishude/oh-my-lazier/go/internal/packets"
	"github.com/jackc/pgx/v5"
)

// MaxLzReceiveDeliveryAttempts bounds how many times a reverting lzReceive is
// re-broadcast before the job is parked for manual review. Each attempt is a
// paid on-chain transaction, so an unbounded loop would drain the executor
// signer against a receiver that reverts at execution. The cap is enforced on
// both retry drivers: the executor deliverer and the txmgr receipt-retry path.
const MaxLzReceiveDeliveryAttempts = 5

// ExecutorJobRecord records an OpenExecutor assignment for a known packet.
type ExecutorJobRecord struct {
	GUID        common.Hash
	AssignedFee *big.Int
	Status      string
	LastError   string
	RetryCount  int64
	// ReceiveTxHash is the last lzReceive tx hash applied to this job (success
	// or counted failure); zero until a receive outcome has been recorded.
	ReceiveTxHash common.Hash
}

// ExecutorWorkItem is a packet plus its executor job state selected for processing.
type ExecutorWorkItem struct {
	Packet PacketRecord
	Job    ExecutorJobRecord
}

// UpsertExecutorJob persists executor assignment state for a packet.
func (s *Store) UpsertExecutorJob(ctx context.Context, job ExecutorJobRecord) error {
	if job.GUID == (common.Hash{}) {
		return errors.New("executor job guid is required")
	}
	if job.AssignedFee != nil && job.AssignedFee.Sign() < 0 {
		return errors.New("executor assigned fee must be non-negative")
	}
	if job.Status == "" {
		return errors.New("executor job status is required")
	}
	var fee any
	if job.AssignedFee != nil {
		fee = job.AssignedFee.String()
	}
	var lastError any
	if job.LastError != "" {
		lastError = job.LastError
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO executor_jobs (guid, assigned, assigned_fee, status, last_error)
		VALUES ($1, true, $2, $3, $4)
			ON CONFLICT (guid) DO UPDATE SET
				assigned = true,
				assigned_fee = EXCLUDED.assigned_fee,
				updated_at = now()
		`, job.GUID.Bytes(), fee, job.Status, lastError)
	return err
}

// GetExecutorJob returns one executor job by packet GUID.
func (s *Store) GetExecutorJob(ctx context.Context, guid common.Hash) (ExecutorJobRecord, error) {
	if guid == (common.Hash{}) {
		return ExecutorJobRecord{}, errors.New("executor job guid is required")
	}
	var job ExecutorJobRecord
	var feeText *string
	var receiveTxHash []byte
	if err := s.pool.QueryRow(ctx, `
		SELECT guid, assigned_fee::text, status, COALESCE(last_error, ''), retry_count, receive_tx_hash
		FROM executor_jobs
		WHERE guid = $1
	`, guid.Bytes()).Scan(&job.GUID, &feeText, &job.Status, &job.LastError, &job.RetryCount, &receiveTxHash); err != nil {
		return ExecutorJobRecord{}, err
	}
	if len(receiveTxHash) == common.HashLength {
		job.ReceiveTxHash = common.BytesToHash(receiveTxHash)
	}
	if feeText != nil {
		fee, err := bigutil.ParseDecimal("executor assigned fee", *feeText)
		if err != nil {
			return ExecutorJobRecord{}, fmt.Errorf("executor assigned fee %q is invalid", *feeText)
		}
		job.AssignedFee = fee
	}
	return job, nil
}

// ListExecutorWork returns executor jobs in one durable status with the packet data needed to act.
func (s *Store) ListExecutorWork(ctx context.Context, status string, limit int) ([]ExecutorWorkItem, error) {
	if status == "" {
		return nil, errors.New("executor status is required")
	}
	if limit <= 0 {
		return nil, errors.New("executor work limit must be positive")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT
			p.guid, p.src_eid, p.dst_eid, p.nonce::text, p.sender, p.receiver,
			p.send_lib, p.src_tx_hash, p.src_block_number, p.src_log_index,
			p.encoded_packet, p.packet_header, p.message, p.payload_hash,
			p.options, p.status, ej.assigned_fee::text, ej.status, ej.retry_count
		FROM executor_jobs ej
		JOIN packets p ON p.guid = ej.guid
		WHERE ej.status = $1 AND (ej.next_retry_at IS NULL OR ej.next_retry_at <= now())
			AND EXISTS (
				SELECT 1 FROM pathways pw
				WHERE pw.src_eid = p.src_eid AND pw.dst_eid = p.dst_eid
					AND pw.src_oapp = p.sender AND pw.dst_oapp = p.receiver
					AND pw.enabled AND NOT pw.paused
			)
			AND NOT EXISTS (SELECT 1 FROM chains c WHERE c.eid IN (p.src_eid, p.dst_eid) AND (NOT c.enabled OR c.paused))
		ORDER BY ej.updated_at, ej.guid
		LIMIT $2
	`, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	work := make([]ExecutorWorkItem, 0)
	for rows.Next() {
		var row executorWorkRow
		if err := rows.Scan(
			&row.GUID,
			&row.SrcEID,
			&row.DstEID,
			&row.Nonce,
			&row.Sender,
			&row.Receiver,
			&row.SendLib,
			&row.SrcTxHash,
			&row.SrcBlockNumber,
			&row.SrcLogIndex,
			&row.EncodedPacket,
			&row.PacketHeader,
			&row.Message,
			&row.PayloadHash,
			&row.Options,
			&row.PacketStatus,
			&row.AssignedFee,
			&row.JobStatus,
			&row.RetryCount,
		); err != nil {
			return nil, err
		}
		item, err := row.toExecutorWorkItem()
		if err != nil {
			return nil, err
		}
		work = append(work, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return work, nil
}

// DeferExecutorJob postpones a non-terminal executor job without changing its durable status.
func (s *Store) DeferExecutorJob(ctx context.Context, guid common.Hash, expectedStatus string, delay time.Duration) error {
	if guid == (common.Hash{}) {
		return errors.New("executor job guid is required")
	}
	if expectedStatus == "" {
		return errors.New("executor status is required")
	}
	if delay <= 0 {
		return errors.New("executor defer delay must be positive")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE executor_jobs
		SET next_retry_at = now() + $3::interval, updated_at = now()
		WHERE guid = $1 AND status = $2
	`, guid.Bytes(), expectedStatus, pgInterval(delay))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("executor job %s is not in status %s", guid, expectedStatus)
	}
	return nil
}

// EnqueueExecutorTx inserts an outbox tx and advances packet/job status atomically.
func (s *Store) EnqueueExecutorTx(ctx context.Context, guid common.Hash, expectedStatus, nextStatus string, request TxRequest) (int64, error) {
	if guid == (common.Hash{}) {
		return 0, errors.New("executor job guid is required")
	}
	if expectedStatus == "" || nextStatus == "" {
		return 0, errors.New("executor transition statuses are required")
	}
	if len(request.GUID) == 0 {
		request.GUID = guid.Bytes()
	}
	if len(request.GUID) != common.HashLength || common.BytesToHash(request.GUID) != guid {
		return 0, errors.New("tx request guid does not match executor job")
	}
	if request.ChainEID == 0 {
		return 0, errors.New("chain eid is required")
	}
	if request.Purpose == "" {
		return 0, errors.New("purpose is required")
	}
	if request.To == (common.Address{}) {
		return 0, errors.New("to address is required")
	}
	if request.SignerID == "" {
		return 0, errors.New("signer id is required")
	}
	value := request.Value
	if value == nil {
		value = new(big.Int)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var currentStatus string
	err = tx.QueryRow(ctx, `
		SELECT status
		FROM executor_jobs
		WHERE guid = $1
		FOR UPDATE
	`, guid.Bytes()).Scan(&currentStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("executor job %s not found", guid)
	}
	if err != nil {
		return 0, err
	}
	if currentStatus != expectedStatus {
		return 0, fmt.Errorf("executor job %s status is %s, want %s", guid, currentStatus, expectedStatus)
	}
	// Refuse to enqueue new spend for a paused/disabled pathway or chain; the
	// job keeps its current status and work selection re-offers it once the
	// scope is active again.
	if err := lockTxSendScope(ctx, tx, request.ChainEID, request.Purpose, request.GUID); err != nil {
		return 0, err
	}

	var id int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO tx_outbox (
			chain_eid, purpose, guid, to_address, calldata, value,
			signer_id, status
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id
	`, request.ChainEID, request.Purpose, optionalBytes(request.GUID), addressBytes(request.To), request.Calldata, value.String(), request.SignerID, TxStatusQueued).Scan(&id); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE executor_jobs
		SET status = $1, updated_at = now()
		WHERE guid = $2
	`, nextStatus, guid.Bytes()); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE packets
		SET status = $1, updated_at = now()
		WHERE guid = $2
	`, nextStatus, guid.Bytes()); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return id, nil
}

// MarkExecutorWaitingDVNVerification records that a job is assigned but not yet commit-verifiable.
func (s *Store) MarkExecutorWaitingDVNVerification(ctx context.Context, guid common.Hash, expectedStatus string) error {
	return s.updateExecutorStatus(ctx, executorStatusUpdate{
		GUID:           guid,
		ExpectedStatus: expectedStatus,
		NextStatus:     string(packets.ExecutorWaitingDVNVerification),
	})
}

// MarkExecutorVerifiable records that destination ULN state allows commitVerification.
func (s *Store) MarkExecutorVerifiable(ctx context.Context, guid common.Hash, expectedStatus string) error {
	return s.updateExecutorStatus(ctx, executorStatusUpdate{
		GUID:           guid,
		ExpectedStatus: expectedStatus,
		NextStatus:     string(packets.ExecutorVerifiable),
	})
}

// MarkExecutorCommitted records a successful commitVerification receipt.
func (s *Store) MarkExecutorCommitted(ctx context.Context, guid, txHash common.Hash) error {
	if txHash == (common.Hash{}) {
		return errors.New("executor commit tx hash is required")
	}
	return s.updateExecutorStatus(ctx, executorStatusUpdate{
		GUID:              guid,
		ExpectedStatus:    string(packets.ExecutorCommitTxEnqueued),
		NextStatus:        string(packets.ExecutorCommitted),
		CommitTxHashBytes: txHash.Bytes(),
	})
}

// MarkExecutorCommittedFromChain records commitVerification already completed on chain without a local tx hash.
func (s *Store) MarkExecutorCommittedFromChain(ctx context.Context, guid common.Hash, expectedStatus string) error {
	return s.updateExecutorStatus(ctx, executorStatusUpdate{
		GUID:           guid,
		ExpectedStatus: expectedStatus,
		NextStatus:     string(packets.ExecutorCommitted),
	})
}

// MarkExecutorCommittedObserved records a destination PacketVerified event observed by the indexer.
func (s *Store) MarkExecutorCommittedObserved(ctx context.Context, guid, txHash common.Hash, expectedStatus string) error {
	if txHash == (common.Hash{}) {
		return errors.New("executor commit tx hash is required")
	}
	return s.updateExecutorStatus(ctx, executorStatusUpdate{
		GUID:              guid,
		ExpectedStatus:    expectedStatus,
		NextStatus:        string(packets.ExecutorCommitted),
		CommitTxHashBytes: txHash.Bytes(),
	})
}

// MarkExecutorExecutable records that endpoint state allows lzReceive delivery.
func (s *Store) MarkExecutorExecutable(ctx context.Context, guid common.Hash) error {
	return s.updateExecutorStatus(ctx, executorStatusUpdate{
		GUID:           guid,
		ExpectedStatus: string(packets.ExecutorCommitted),
		NextStatus:     string(packets.ExecutorExecutable),
	})
}

// MarkExecutorManualReview records a deterministic executor failure that requires operator action.
func (s *Store) MarkExecutorManualReview(ctx context.Context, guid common.Hash, expectedStatus, reason string) error {
	if reason == "" {
		return errors.New("executor manual review reason is required")
	}
	return s.updateExecutorStatus(ctx, executorStatusUpdate{
		GUID:           guid,
		ExpectedStatus: expectedStatus,
		NextStatus:     string(packets.ExecutorManualReview),
		LastError:      reason,
	})
}

// MarkExecutorDelivered records a successful lzReceive receipt or PacketDelivered event.
func (s *Store) MarkExecutorDelivered(ctx context.Context, guid, txHash common.Hash) error {
	if txHash == (common.Hash{}) {
		return errors.New("executor receive tx hash is required")
	}
	return s.updateExecutorStatus(ctx, executorStatusUpdate{
		GUID:               guid,
		ExpectedStatus:     string(packets.ExecutorLzReceiveTxEnqueued),
		NextStatus:         string(packets.ExecutorDelivered),
		ReceiveTxHashBytes: txHash.Bytes(),
	})
}

// MarkExecutorDeliveredFromChain records lzReceive already completed on chain without a local tx hash.
func (s *Store) MarkExecutorDeliveredFromChain(ctx context.Context, guid common.Hash, expectedStatus string) error {
	return s.updateExecutorStatus(ctx, executorStatusUpdate{
		GUID:           guid,
		ExpectedStatus: expectedStatus,
		NextStatus:     string(packets.ExecutorDelivered),
	})
}

// MarkExecutorDeliveredObserved records a destination PacketDelivered event observed by the indexer.
func (s *Store) MarkExecutorDeliveredObserved(ctx context.Context, guid, txHash common.Hash, expectedStatus string) error {
	if txHash == (common.Hash{}) {
		return errors.New("executor receive tx hash is required")
	}
	return s.updateExecutorStatus(ctx, executorStatusUpdate{
		GUID:               guid,
		ExpectedStatus:     expectedStatus,
		NextStatus:         string(packets.ExecutorDelivered),
		ReceiveTxHashBytes: txHash.Bytes(),
	})
}

// MarkExecutorReceiveFailed records an LzReceiveAlert or failed lzReceive receipt.
// Each failing tx hash is counted at most once per job: the receipt applier and
// the destination-event observer both report the same on-chain failure, and a
// crash-replayed receipt may arrive after the deliverer already re-enqueued the
// job, so a repeat of an already-counted hash is a no-op instead of charging
// the retry budget twice and failing the in-flight attempt.
func (s *Store) MarkExecutorReceiveFailed(ctx context.Context, guid, txHash common.Hash, reason string) error {
	if txHash == (common.Hash{}) {
		return errors.New("executor receive tx hash is required")
	}
	return s.updateExecutorStatus(ctx, executorStatusUpdate{
		GUID:                guid,
		ExpectedStatus:      string(packets.ExecutorLzReceiveTxEnqueued),
		NextStatus:          string(packets.ExecutorLzReceiveFailed),
		ReceiveTxHashBytes:  txHash.Bytes(),
		LastError:           reason,
		IncrementRetryCount: true,
	})
}

// MarkExecutorReceiveFailedObserved records a destination LzReceiveAlert event observed by the indexer.
func (s *Store) MarkExecutorReceiveFailedObserved(ctx context.Context, guid, txHash common.Hash, expectedStatus, reason string) error {
	if txHash == (common.Hash{}) {
		return errors.New("executor receive tx hash is required")
	}
	return s.updateExecutorStatus(ctx, executorStatusUpdate{
		GUID:                guid,
		ExpectedStatus:      expectedStatus,
		NextStatus:          string(packets.ExecutorLzReceiveFailed),
		ReceiveTxHashBytes:  txHash.Bytes(),
		LastError:           reason,
		IncrementRetryCount: true,
	})
}

type executorStatusUpdate struct {
	GUID                common.Hash
	ExpectedStatus      string
	NextStatus          string
	CommitTxHashBytes   []byte
	ReceiveTxHashBytes  []byte
	LastError           string
	IncrementRetryCount bool
}

func (s *Store) updateExecutorStatus(ctx context.Context, update executorStatusUpdate) error {
	if update.GUID == (common.Hash{}) {
		return errors.New("executor job guid is required")
	}
	if update.ExpectedStatus == "" || update.NextStatus == "" {
		return errors.New("executor transition statuses are required")
	}
	if len(update.CommitTxHashBytes) != 0 && len(update.CommitTxHashBytes) != common.HashLength {
		return errors.New("executor commit tx hash must be 32 bytes")
	}
	if len(update.ReceiveTxHashBytes) != 0 && len(update.ReceiveTxHashBytes) != common.HashLength {
		return errors.New("executor receive tx hash must be 32 bytes")
	}
	if update.IncrementRetryCount && len(update.ReceiveTxHashBytes) == 0 {
		return errors.New("executor retry accounting requires the failing receive tx hash")
	}
	lastErrorArg := any(nil)
	if update.LastError != "" {
		lastErrorArg = update.LastError
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Inserting into executor_receive_failures is the counting condition: each
	// failing tx hash charges the retry budget at most once per job, so a
	// crash-replayed receipt or a lagging LzReceiveAlert for an already-counted
	// hash — even one from several attempts back — is a benign no-op instead of
	// double-charging the budget and failing the in-flight attempt. The insert
	// and the status transition commit atomically.
	if update.IncrementRetryCount {
		tag, err := tx.Exec(ctx, `
			INSERT INTO executor_receive_failures (guid, tx_hash)
			VALUES ($1, $2)
			ON CONFLICT (guid, tx_hash) DO NOTHING
		`, update.GUID.Bytes(), update.ReceiveTxHashBytes)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
	}
	tag, err := tx.Exec(ctx, `
		UPDATE executor_jobs
		SET
			status = $1,
			commit_tx_hash = COALESCE($4, commit_tx_hash),
			receive_tx_hash = COALESCE($5, receive_tx_hash),
			last_error = $6,
			retry_count = CASE WHEN $7::boolean THEN retry_count + 1 ELSE retry_count END,
			updated_at = now()
		WHERE guid = $2 AND status = $3
	`, update.NextStatus, update.GUID.Bytes(), update.ExpectedStatus, optionalBytes(update.CommitTxHashBytes), optionalBytes(update.ReceiveTxHashBytes), lastErrorArg, update.IncrementRetryCount)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("executor job %s is not in status %s", update.GUID, update.ExpectedStatus)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE packets
		SET status = $1, updated_at = now()
		WHERE guid = $2
	`, update.NextStatus, update.GUID.Bytes()); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type executorWorkRow struct {
	GUID           []byte
	SrcEID         uint32
	DstEID         uint32
	Nonce          string
	Sender         []byte
	Receiver       []byte
	SendLib        []byte
	SrcTxHash      []byte
	SrcBlockNumber uint64
	SrcLogIndex    uint
	EncodedPacket  []byte
	PacketHeader   []byte
	Message        []byte
	PayloadHash    []byte
	Options        []byte
	PacketStatus   string
	AssignedFee    *string
	JobStatus      string
	RetryCount     int64
}

func (r executorWorkRow) toExecutorWorkItem() (ExecutorWorkItem, error) {
	if len(r.GUID) != common.HashLength {
		return ExecutorWorkItem{}, fmt.Errorf("executor work guid has length %d", len(r.GUID))
	}
	if len(r.Sender) != common.AddressLength {
		return ExecutorWorkItem{}, fmt.Errorf("executor work sender has length %d", len(r.Sender))
	}
	if len(r.Receiver) != common.AddressLength {
		return ExecutorWorkItem{}, fmt.Errorf("executor work receiver has length %d", len(r.Receiver))
	}
	if len(r.SendLib) != common.AddressLength {
		return ExecutorWorkItem{}, fmt.Errorf("executor work send_lib has length %d", len(r.SendLib))
	}
	if len(r.SrcTxHash) != common.HashLength {
		return ExecutorWorkItem{}, fmt.Errorf("executor work src_tx_hash has length %d", len(r.SrcTxHash))
	}
	if len(r.PayloadHash) != common.HashLength {
		return ExecutorWorkItem{}, fmt.Errorf("executor work payload_hash has length %d", len(r.PayloadHash))
	}
	nonce, err := bigutil.ParseRequiredDecimal("packet nonce", &r.Nonce)
	if err != nil {
		return ExecutorWorkItem{}, err
	}
	assignedFee, err := bigutil.ParseOptionalDecimal("assigned_fee", r.AssignedFee)
	if err != nil {
		return ExecutorWorkItem{}, err
	}
	guid := common.BytesToHash(r.GUID)
	packet := PacketRecord{
		GUID:           guid,
		SrcEID:         r.SrcEID,
		DstEID:         r.DstEID,
		Nonce:          nonce,
		Sender:         common.BytesToAddress(r.Sender),
		Receiver:       common.BytesToAddress(r.Receiver),
		SendLib:        common.BytesToAddress(r.SendLib),
		SrcTxHash:      common.BytesToHash(r.SrcTxHash),
		SrcBlockNumber: r.SrcBlockNumber,
		SrcLogIndex:    r.SrcLogIndex,
		EncodedPacket:  bytes.Clone(r.EncodedPacket),
		PacketHeader:   bytes.Clone(r.PacketHeader),
		Message:        bytes.Clone(r.Message),
		PayloadHash:    common.BytesToHash(r.PayloadHash),
		Options:        bytes.Clone(r.Options),
		Status:         r.PacketStatus,
	}
	return ExecutorWorkItem{
		Packet: packet,
		Job: ExecutorJobRecord{
			GUID:        guid,
			AssignedFee: assignedFee,
			Status:      r.JobStatus,
			RetryCount:  r.RetryCount,
		},
	}, nil
}
