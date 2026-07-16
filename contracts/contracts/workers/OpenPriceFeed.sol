// SPDX-License-Identifier: MIT
pragma solidity ^0.8.35;

import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";
import {WorkerErrors} from "../common/WorkerErrors.sol";
import {WorkerTypes} from "../common/WorkerTypes.sol";

/// @title OpenPriceFeed
/// @notice Submitter-managed shared market price snapshots for source-chain workers.
contract OpenPriceFeed is Ownable {
    /// @notice Maximum accepted price freshness window.
    uint64 public constant MAX_PRICE_SNAPSHOT_STALE_AFTER = 1 days;

    /// @notice Price snapshot by destination endpoint ID.
    mapping(uint32 dstEid => WorkerTypes.PriceSnapshot snapshot) public priceSnapshot;

    /// @notice Whether an address may submit price snapshots.
    mapping(address submitter => bool allowed) public submitters;

    /// @notice Emitted when destination price inputs are updated.
    /// @param dstEid Destination endpoint ID.
    /// @param snapshot New price snapshot.
    event PriceSnapshotSet(uint32 indexed dstEid, WorkerTypes.PriceSnapshot snapshot);

    /// @notice Emitted when price submitter authorization changes.
    /// @param submitter Address whose authorization changed.
    /// @param allowed Whether the address may submit price snapshots.
    event SubmitterSet(address indexed submitter, bool allowed);

    /// @notice Initializes price feed ownership and initial submitters.
    /// @param initialOwner Initial owner address.
    /// @param initialSubmitters Addresses allowed to submit price snapshots.
    constructor(address initialOwner, address[] memory initialSubmitters) Ownable(initialOwner) {
        for (uint256 i = 0; i < initialSubmitters.length; i++) {
            _setSubmitter(initialSubmitters[i], true);
        }
    }

    modifier onlySubmitter() {
        if (!submitters[msg.sender]) revert WorkerErrors.UnauthorizedPriceSubmitter(msg.sender);
        _;
    }

    /// @notice Adds or removes an address from the price submitter allowlist.
    /// @param submitter Address whose authorization should change.
    /// @param allowed Whether the address may submit price snapshots.
    function setSubmitter(address submitter, bool allowed) external onlyOwner {
        _setSubmitter(submitter, allowed);
    }

    /// @notice Stores shared market price inputs for destination endpoints.
    /// @dev Each destination snapshot's `updatedAt` must be non-decreasing, so a
    /// reordered or delayed submission cannot overwrite a newer price with an older one.
    /// @param updates Destination endpoint price snapshots to store.
    function setPriceSnapshot(WorkerTypes.PriceSnapshotUpdate[] calldata updates) external onlySubmitter {
        if (updates.length == 0) revert WorkerErrors.InvalidPriceSnapshotBatch();
        for (uint256 i = 0; i < updates.length; i++) {
            WorkerTypes.PriceSnapshotUpdate calldata update = updates[i];
            WorkerTypes.PriceSnapshot calldata snapshot = update.snapshot;
            if (
                snapshot.dstGasPriceInSrcToken == 0 || snapshot.updatedAt == 0 || snapshot.updatedAt > block.timestamp
                    || snapshot.staleAfter == 0 || snapshot.staleAfter > MAX_PRICE_SNAPSHOT_STALE_AFTER
                    || snapshot.updatedAt < priceSnapshot[update.dstEid].updatedAt
            ) {
                revert WorkerErrors.InvalidPriceSnapshot(update.dstEid);
            }
            priceSnapshot[update.dstEid] = snapshot;
            emit PriceSnapshotSet(update.dstEid, snapshot);
        }
    }

    function _setSubmitter(address submitter, bool allowed) internal {
        if (submitter == address(0)) revert WorkerErrors.InvalidPriceSubmitter(submitter);
        submitters[submitter] = allowed;
        emit SubmitterSet(submitter, allowed);
    }
}
