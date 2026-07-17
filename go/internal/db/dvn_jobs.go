package db

import (
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

// DVNJobRecord records an OpenDVN assignment for a known packet.
type DVNJobRecord struct {
	GUID                  common.Hash
	AssignedFee           *big.Int
	ConfirmationsRequired uint64
	Status                string
	QuorumResult          []byte
}

// UpsertDVNJob persists DVN assignment state for a packet.
func (s *Store) UpsertDVNJob(ctx context.Context, job DVNJobRecord) error {
	if job.GUID == (common.Hash{}) {
		return errors.New("dvn job guid is required")
	}
	if job.ConfirmationsRequired == 0 {
		return errors.New("dvn confirmations required is required")
	}
	if job.AssignedFee != nil && job.AssignedFee.Sign() < 0 {
		return errors.New("dvn assigned fee must be non-negative")
	}
	if job.Status == "" {
		return errors.New("dvn job status is required")
	}
	var fee any
	if job.AssignedFee != nil {
		fee = job.AssignedFee.String()
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO dvn_jobs (guid, assigned, assigned_fee, confirmations_required, status)
		VALUES ($1, true, $2, $3, $4)
			ON CONFLICT (guid) DO UPDATE SET
				assigned = true,
				assigned_fee = EXCLUDED.assigned_fee,
				confirmations_required = EXCLUDED.confirmations_required,
				updated_at = now()
		`, job.GUID.Bytes(), fee, job.ConfirmationsRequired, job.Status)
	return err
}

// GetDVNJob returns one persisted DVN job by packet GUID.
func (s *Store) GetDVNJob(ctx context.Context, guid common.Hash) (DVNJobRecord, error) {
	if guid == (common.Hash{}) {
		return DVNJobRecord{}, errors.New("dvn job guid is required")
	}
	var row struct {
		guid                  []byte
		assignedFee           *string
		confirmationsRequired uint64
		status                string
	}
	err := s.pool.QueryRow(ctx, `
		SELECT guid, assigned_fee::text, confirmations_required, status
		FROM dvn_jobs
		WHERE guid = $1
	`, guid.Bytes()).Scan(&row.guid, &row.assignedFee, &row.confirmationsRequired, &row.status)
	if err != nil {
		return DVNJobRecord{}, err
	}
	if len(row.guid) != common.HashLength {
		return DVNJobRecord{}, fmt.Errorf("dvn job guid has length %d", len(row.guid))
	}
	assignedFee, err := bigutil.ParseOptionalDecimal("dvn assigned fee", row.assignedFee)
	if err != nil {
		return DVNJobRecord{}, fmt.Errorf("dvn assigned fee %q is invalid", *row.assignedFee)
	}
	return DVNJobRecord{
		GUID:                  common.BytesToHash(row.guid),
		AssignedFee:           assignedFee,
		ConfirmationsRequired: row.confirmationsRequired,
		Status:                row.status,
	}, nil
}

// ListDVNWork returns DVN jobs in one durable status with packet data needed to verify.
func (s *Store) ListDVNWork(ctx context.Context, status string, limit int) ([]DVNWorkItem, error) {
	if status == "" {
		return nil, errors.New("dvn status is required")
	}
	if limit <= 0 {
		return nil, errors.New("dvn work limit must be positive")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT
			p.guid, p.src_eid, p.dst_eid, p.nonce::text, p.sender, p.receiver,
			p.send_lib, p.src_tx_hash, p.src_block_number, p.src_log_index,
			p.encoded_packet, p.packet_header, p.message, p.payload_hash,
			p.options, p.status, dj.assigned_fee::text, dj.confirmations_required, dj.status,
			COALESCE(dj.quorum_result::text, '')
		FROM dvn_jobs dj
		JOIN packets p ON p.guid = dj.guid
		WHERE dj.status = $1 AND (dj.next_retry_at IS NULL OR dj.next_retry_at <= now())
			AND EXISTS (
				SELECT 1 FROM pathways pw
				WHERE pw.src_eid = p.src_eid AND pw.dst_eid = p.dst_eid
					AND pw.src_oapp = p.sender AND pw.dst_oapp = p.receiver
					AND pw.enabled AND NOT pw.paused
			)
			AND NOT EXISTS (SELECT 1 FROM chains c WHERE c.eid IN (p.src_eid, p.dst_eid) AND (NOT c.enabled OR c.paused))
		ORDER BY dj.updated_at, dj.guid
		LIMIT $2
	`, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	work := make([]DVNWorkItem, 0)
	for rows.Next() {
		var row dvnWorkRow
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
			&row.ConfirmationsRequired,
			&row.JobStatus,
			&row.QuorumResult,
		); err != nil {
			return nil, err
		}
		item, err := row.toDVNWorkItem()
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

// DeferDVNJob postpones a non-terminal DVN job without changing its durable status.
func (s *Store) DeferDVNJob(ctx context.Context, guid common.Hash, expectedStatus string, delay time.Duration) error {
	if guid == (common.Hash{}) {
		return errors.New("dvn job guid is required")
	}
	if expectedStatus == "" {
		return errors.New("dvn status is required")
	}
	if delay <= 0 {
		return errors.New("dvn defer delay must be positive")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE dvn_jobs
		SET next_retry_at = now() + $3::interval, updated_at = now()
		WHERE guid = $1 AND status = $2
	`, guid.Bytes(), expectedStatus, pgInterval(delay))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("dvn job %s is not in status %s", guid, expectedStatus)
	}
	return nil
}

// MarkDVNWaitingConfirmations records that the source packet has not reached required confirmations yet.
func (s *Store) MarkDVNWaitingConfirmations(ctx context.Context, guid common.Hash, expectedStatus string) error {
	return s.updateDVNStatus(ctx, dvnStatusUpdate{GUID: guid, ExpectedStatus: expectedStatus, NextStatus: string(packets.DVNWaitingConfirmations)})
}

// MarkDVNQuorumChecking records that a DVN job has enough source confirmations for quorum checks.
func (s *Store) MarkDVNQuorumChecking(ctx context.Context, guid common.Hash, expectedStatus string) error {
	return s.updateDVNStatus(ctx, dvnStatusUpdate{GUID: guid, ExpectedStatus: expectedStatus, NextStatus: string(packets.DVNQuorumChecking)})
}

// MarkDVNReadyToVerify records a quorum report for a packet ready for shadow or active verification.
func (s *Store) MarkDVNReadyToVerify(ctx context.Context, guid common.Hash, expectedStatus string, quorumResult []byte) error {
	if len(quorumResult) == 0 {
		return errors.New("dvn quorum result is required")
	}
	return s.updateDVNStatus(ctx, dvnStatusUpdate{GUID: guid, ExpectedStatus: expectedStatus, NextStatus: string(packets.DVNReadyToVerify), QuorumResult: quorumResult})
}

// MarkDVNWouldVerify records a shadow-mode report for a packet that passed quorum checks.
func (s *Store) MarkDVNWouldVerify(ctx context.Context, guid common.Hash, expectedStatus string, quorumResult []byte) error {
	if len(quorumResult) == 0 {
		return errors.New("dvn quorum result is required")
	}
	return s.updateDVNStatus(ctx, dvnStatusUpdate{GUID: guid, ExpectedStatus: expectedStatus, NextStatus: string(packets.DVNWouldVerify), QuorumResult: quorumResult})
}

// EnqueueDVNVerifyTx inserts an active DVN verify transaction and advances the job status atomically.
func (s *Store) EnqueueDVNVerifyTx(ctx context.Context, guid common.Hash, expectedStatus, nextStatus string, request TxRequest, quorumResult []byte) (int64, error) {
	if guid == (common.Hash{}) {
		return 0, errors.New("dvn job guid is required")
	}
	if expectedStatus == "" || nextStatus == "" {
		return 0, errors.New("dvn transition statuses are required")
	}
	if len(request.GUID) == 0 {
		request.GUID = guid.Bytes()
	}
	if len(request.GUID) != common.HashLength || common.BytesToHash(request.GUID) != guid {
		return 0, errors.New("tx request guid does not match dvn job")
	}
	if len(quorumResult) == 0 {
		return 0, errors.New("dvn quorum result is required")
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
		FROM dvn_jobs
		WHERE guid = $1
		FOR UPDATE
	`, guid.Bytes()).Scan(&currentStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("dvn job %s not found", guid)
	}
	if err != nil {
		return 0, err
	}
	if currentStatus != expectedStatus {
		return 0, fmt.Errorf("dvn job %s status is %s, want %s", guid, currentStatus, expectedStatus)
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
		UPDATE dvn_jobs
		SET status = $1, quorum_result = $4::jsonb, updated_at = now()
		WHERE guid = $2 AND status = $3
	`, nextStatus, guid.Bytes(), expectedStatus, string(quorumResult)); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return id, nil
}

// MarkDVNVerified records a successful active ReceiveUln302.verify receipt.
func (s *Store) MarkDVNVerified(ctx context.Context, guid, txHash common.Hash) error {
	if txHash == (common.Hash{}) {
		return errors.New("dvn verify tx hash is required")
	}
	return s.updateDVNStatus(ctx, dvnStatusUpdate{
		GUID:              guid,
		ExpectedStatus:    string(packets.DVNVerifyTxEnqueued),
		NextStatus:        string(packets.DVNVerified),
		VerifyTxHashBytes: txHash.Bytes(),
	})
}

// MarkDVNVerifiedFromChain records that ReceiveUln302 already has this DVN verification without a local tx hash.
func (s *Store) MarkDVNVerifiedFromChain(ctx context.Context, guid common.Hash, expectedStatus string, quorumResult []byte) error {
	if len(quorumResult) == 0 {
		return errors.New("dvn quorum result is required")
	}
	return s.updateDVNStatus(ctx, dvnStatusUpdate{
		GUID:           guid,
		ExpectedStatus: expectedStatus,
		NextStatus:     string(packets.DVNVerified),
		QuorumResult:   quorumResult,
	})
}

// MarkDVNVerifiedObserved records a destination PayloadVerified event observed by the indexer.
func (s *Store) MarkDVNVerifiedObserved(ctx context.Context, guid, txHash common.Hash, expectedStatus string) error {
	if txHash == (common.Hash{}) {
		return errors.New("dvn verify tx hash is required")
	}
	return s.updateDVNStatus(ctx, dvnStatusUpdate{
		GUID:              guid,
		ExpectedStatus:    expectedStatus,
		NextStatus:        string(packets.DVNVerified),
		VerifyTxHashBytes: txHash.Bytes(),
	})
}

// MarkDVNQuorumConflict records a quorum verification conflict requiring operator review.
func (s *Store) MarkDVNQuorumConflict(ctx context.Context, guid common.Hash, expectedStatus, reason string, quorumResult []byte) error {
	if reason == "" {
		return errors.New("dvn quorum conflict reason is required")
	}
	return s.updateDVNStatus(ctx, dvnStatusUpdate{GUID: guid, ExpectedStatus: expectedStatus, NextStatus: string(packets.DVNQuorumConflict), LastError: reason, QuorumResult: quorumResult})
}

// MarkDVNReorgDetected records that the previously indexed source receipt disappeared.
func (s *Store) MarkDVNReorgDetected(ctx context.Context, guid common.Hash, expectedStatus, reason string, quorumResult []byte) error {
	if reason == "" {
		return errors.New("dvn reorg reason is required")
	}
	return s.updateDVNStatus(ctx, dvnStatusUpdate{GUID: guid, ExpectedStatus: expectedStatus, NextStatus: string(packets.DVNReorgDetected), LastError: reason, QuorumResult: quorumResult})
}

// MarkDVNManualReview parks a DVN job for operator review without pausing the
// pathway: an operator-canceled verify transaction is not configuration drift,
// so other packets on the pathway keep flowing.
func (s *Store) MarkDVNManualReview(ctx context.Context, guid common.Hash, expectedStatus, reason string) error {
	if reason == "" {
		return errors.New("dvn manual review reason is required")
	}
	return s.updateDVNStatus(ctx, dvnStatusUpdate{
		GUID:           guid,
		ExpectedStatus: expectedStatus,
		NextStatus:     string(packets.DVNManualReview),
		LastError:      reason,
	})
}

// MarkDVNManualReviewAndPausePathway atomically stops a job and its pathway after deterministic destination config drift.
func (s *Store) MarkDVNManualReviewAndPausePathway(ctx context.Context, guid common.Hash, expectedStatus, reason string, quorumResult []byte) error {
	if guid == (common.Hash{}) {
		return errors.New("dvn job guid is required")
	}
	if expectedStatus == "" {
		return errors.New("dvn expected status is required")
	}
	if reason == "" {
		return errors.New("dvn manual review reason is required")
	}
	quorumResultArg := any(nil)
	if len(quorumResult) != 0 {
		quorumResultArg = string(quorumResult)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `
		UPDATE dvn_jobs
		SET
			status = $1,
			last_error = $4,
			quorum_result = COALESCE($5::jsonb, quorum_result),
			next_retry_at = NULL,
			updated_at = now()
		WHERE guid = $2 AND status = $3
	`, string(packets.DVNManualReview), guid.Bytes(), expectedStatus, reason, quorumResultArg)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("dvn job %s is not in status %s", guid, expectedStatus)
	}
	tag, err = tx.Exec(ctx, `
		UPDATE pathways AS pathway
		SET paused = true
		FROM packets AS packet
		WHERE
			packet.guid = $1
			AND pathway.src_eid = packet.src_eid
			AND pathway.dst_eid = packet.dst_eid
			AND pathway.src_oapp = packet.sender
			AND pathway.dst_oapp = packet.receiver
	`, guid.Bytes())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("pathway for packet %s was not found", guid)
	}
	return tx.Commit(ctx)
}

type dvnStatusUpdate struct {
	GUID              common.Hash
	ExpectedStatus    string
	NextStatus        string
	LastError         string
	QuorumResult      []byte
	VerifyTxHashBytes []byte
}

func (s *Store) updateDVNStatus(ctx context.Context, update dvnStatusUpdate) error {
	if update.GUID == (common.Hash{}) {
		return errors.New("dvn job guid is required")
	}
	if update.ExpectedStatus == "" || update.NextStatus == "" {
		return errors.New("dvn transition statuses are required")
	}
	if len(update.VerifyTxHashBytes) != 0 && len(update.VerifyTxHashBytes) != common.HashLength {
		return errors.New("dvn verify tx hash must be 32 bytes")
	}
	lastErrorArg := any(nil)
	if update.LastError != "" {
		lastErrorArg = update.LastError
	}
	quorumResultArg := any(nil)
	if len(update.QuorumResult) != 0 {
		quorumResultArg = string(update.QuorumResult)
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE dvn_jobs
		SET
			status = $1,
			last_error = $4,
			quorum_result = COALESCE($5::jsonb, quorum_result),
			verify_tx_hash = COALESCE($6, verify_tx_hash),
			updated_at = now()
		WHERE guid = $2 AND status = $3
	`, update.NextStatus, update.GUID.Bytes(), update.ExpectedStatus, lastErrorArg, quorumResultArg, optionalBytes(update.VerifyTxHashBytes))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("dvn job %s is not in status %s", update.GUID, update.ExpectedStatus)
	}
	return nil
}

// DVNWorkItem is a packet plus its DVN job state selected for processing.
type DVNWorkItem struct {
	Packet PacketRecord
	Job    DVNJobRecord
}

type dvnWorkRow struct {
	GUID                  []byte
	SrcEID                uint32
	DstEID                uint32
	Nonce                 string
	Sender                []byte
	Receiver              []byte
	SendLib               []byte
	SrcTxHash             []byte
	SrcBlockNumber        uint64
	SrcLogIndex           uint
	EncodedPacket         []byte
	PacketHeader          []byte
	Message               []byte
	PayloadHash           []byte
	Options               []byte
	PacketStatus          string
	AssignedFee           *string
	ConfirmationsRequired uint64
	JobStatus             string
	QuorumResult          string
}

func (r dvnWorkRow) toDVNWorkItem() (DVNWorkItem, error) {
	packet, err := packetRow{
		GUID:           r.GUID,
		SrcEID:         r.SrcEID,
		DstEID:         r.DstEID,
		Nonce:          r.Nonce,
		Sender:         r.Sender,
		Receiver:       r.Receiver,
		SendLib:        r.SendLib,
		SrcTxHash:      r.SrcTxHash,
		SrcBlockNumber: r.SrcBlockNumber,
		SrcLogIndex:    r.SrcLogIndex,
		EncodedPacket:  r.EncodedPacket,
		PacketHeader:   r.PacketHeader,
		Message:        r.Message,
		PayloadHash:    r.PayloadHash,
		Options:        r.Options,
		Status:         r.PacketStatus,
	}.toPacketRecord()
	if err != nil {
		return DVNWorkItem{}, err
	}
	if r.ConfirmationsRequired == 0 {
		return DVNWorkItem{}, fmt.Errorf("dvn work confirmations required is zero")
	}
	assignedFee, err := bigutil.ParseOptionalDecimal("assigned_fee", r.AssignedFee)
	if err != nil {
		return DVNWorkItem{}, err
	}
	return DVNWorkItem{
		Packet: packet,
		Job: DVNJobRecord{
			GUID:                  packet.GUID,
			AssignedFee:           assignedFee,
			ConfirmationsRequired: r.ConfirmationsRequired,
			Status:                r.JobStatus,
			QuorumResult:          []byte(r.QuorumResult),
		},
	}, nil
}
