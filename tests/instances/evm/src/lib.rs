//!
//! These tests are focused on EVM execution.
//!
#![cfg(test)]

use rig::alloy::consensus::TxEip4844;
use rig::alloy::consensus::TxLegacy;
use rig::alloy::hex::FromHex;
use rig::alloy::primitives::FixedBytes;
use rig::alloy_sol_types::sol;
use rig::alloy_sol_types::SolCall;
use rig::zksync_os_interface::types::ExecutionOutput;
use rig::zksync_os_interface::types::ExecutionResult;
use rig::BlockContext;
use rig::Chain;
use rig::{
    alloy::primitives::address,
    ruint::aliases::{B160, U256},
};

#[test]
fn test_blockhash() {
    // Check that all the last 256 block hashes are accessible and the previous to that/
    // current are out of range.
    let mut chain = Chain::empty(None);

    // Code of contract used for testing
    //   contract BlockhashTester {
    //     error ChainTooShort(uint256 currentBlock);
    //     error CurrentBlockHashNonZero(bytes32 got);
    //     error OutOfRange257NonZero(bytes32 got);
    //     error Mismatch(uint256 blockNumber, bytes32 got, bytes32 expected);

    //     /// Runs all checks:
    //     /// - blockhash(current) == 0
    //     /// - blockhash(current - 257) == 0
    //     /// - for i in 1..=256: blockhash(current - i) == bytes32(current - i)
    //     /// Reverts with details on the first failure; returns true if all pass.
    //     function checkAll() external view returns (bool) {
    //         uint256 cur = block.number;
    //         // Need to be able to reference (cur - 257)
    //         if (cur < 257) revert ChainTooShort(cur);

    //         // Current block must be 0
    //         {
    //             bytes32 got = blockhash(cur);
    //             if (got != bytes32(0)) revert CurrentBlockHashNonZero(got);
    //         }

    //         // 257th previous is out of range -> 0
    //         {
    //             bytes32 got = blockhash(cur - 257);
    //             if (got != bytes32(0)) revert OutOfRange257NonZero(got);
    //         }

    //         // Previous 256 blocks must equal their block numbers
    //         unchecked {
    //             for (uint256 i = 1; i <= 256; i++) {
    //                 uint256 bn = cur - i;
    //                 bytes32 expected = bytes32(bn);
    //                 bytes32 got = blockhash(bn);
    //                 if (got != expected) revert Mismatch(bn, got, expected);
    //             }
    //         }

    //         return true;
    //     }
    // }
    let bytecode = hex::decode("608060405234801561000f575f5ffd5b5060043610610029575f3560e01c806379a77d7c1461002d575b5f5ffd5b61003561004b565b60405161004291906101d7565b60405180910390f35b5f5f43905061010181101561009757806040517f3ba2072200000000000000000000000000000000000000000000000000000000815260040161008e9190610208565b60405180910390fd5b5f814090505f5f1b81146100e257806040517fe42affce0000000000000000000000000000000000000000000000000000000081526004016100d99190610239565b60405180910390fd5b505f610101826100f2919061027f565b4090505f5f1b811461013b57806040517fa475f65e0000000000000000000000000000000000000000000000000000000081526004016101329190610239565b60405180910390fd5b505f600190505b61010081116101b4575f81830390505f815f1b90505f824090508181146101a4578281836040517f9ba8c2e500000000000000000000000000000000000000000000000000000000815260040161019b939291906102b2565b60405180910390fd5b5050508080600101915050610142565b50600191505090565b5f8115159050919050565b6101d1816101bd565b82525050565b5f6020820190506101ea5f8301846101c8565b92915050565b5f819050919050565b610202816101f0565b82525050565b5f60208201905061021b5f8301846101f9565b92915050565b5f819050919050565b61023381610221565b82525050565b5f60208201905061024c5f83018461022a565b92915050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52601160045260245ffd5b5f610289826101f0565b9150610294836101f0565b92508282039050818111156102ac576102ab610252565b5b92915050565b5f6060820190506102c55f8301866101f9565b6102d2602083018561022a565b6102df604083018461022a565b94935050505056fea2646970667358221220e82914bf0a48f1834867f2e80c5e5c1acae5c38369cb00ae1216972f7cf4936b64736f6c634300081e0033").unwrap();

    sol! { function checkAll() external view returns (bool); }

    let calldata = {
        let call = checkAllCall {};
        call.abi_encode()
    };

    let wallet = chain.random_signer();

    let to = address!("0x1000000000000000000000000000000000000000");

    // Set a reasonable balance that would be sufficient for normal transactions
    chain.set_balance(
        B160::from_be_bytes(wallet.address().into_array()),
        U256::from(1_000_000_000_000_000_u64),
    );
    chain.set_evm_bytecode(B160::from_be_bytes(to.into_array()), &bytecode);

    let tx = rig::utils::sign_and_encode_alloy_tx(
        TxLegacy {
            chain_id: 37u64.into(),
            nonce: 0,
            gas_price: 1000,
            gas_limit: 500_000,
            to: rig::alloy::primitives::TxKind::Call(to),
            value: Default::default(),
            input: calldata.into(),
        },
        &wallet,
    );

    // We set block number to 300, to have > 256 block hashes to query
    let block_number = 300;
    chain.set_last_block_number(block_number - 1);

    let mut block_hashes = [U256::ZERO; 256];
    for i in 0..256 {
        let n = block_number - (256 - i);
        block_hashes[i as usize] = U256::from(n);
    }
    chain.set_block_hashes(block_hashes);

    let run_config = rig::chain::RunConfig {
        app: Some("for_tests".to_string()),
        only_forward: false,
        check_storage_diff_hashes: true,
        ..Default::default()
    };
    let result = chain.run_block(vec![tx], None, Some(run_config));
    assert!(result.tx_results[0].is_ok(),);

    let tx_result = result.tx_results[0].as_ref().unwrap();
    assert!(tx_result.is_success(), "Transaction should be successful");
    // check the output to ensure the contract was called
    match &tx_result.execution_result {
        ExecutionResult::Success(ExecutionOutput::Call(out)) => assert_eq!(
            out,
            &U256::ONE.to_be_bytes_vec(),
            "Output data doesn't match"
        ),
        _ => panic!("Execution result must be a successful call"),
    }
}

#[test]
fn test_eip4844_blobhash() {
    let mut chain = Chain::empty(None);

    // Test funding signer
    let wallet = chain.random_signer();

    // Deploy address for the contract in this test (set to some deterministic slot)
    let inspector_addr = address!("0000000000000000000000000000000000010003");

    // Minimal helper for EIP-4844 tests.
    // contract BlobHashEcho {
    //     // Single entrypoint: takes an index, executes BLOBHASH, returns the 32-byte result as raw returndata.
    //     function main(uint256 index) external payable {
    //         assembly {
    //             let h := blobhash(index)
    //             mstore(0x00, h)
    //             return(0x00, 0x20)
    //         }
    //     }
    // }
    let bytecode: Vec<u8> = hex::decode("608060405260043610601b575f3560e01c8063ab3ae25514601f575b5f5ffd5b60356004803603810190603191906073565b6037565b005b8049805f5260205ff35b5f5ffd5b5f819050919050565b6055816045565b8114605e575f5ffd5b50565b5f81359050606d81604e565b92915050565b5f6020828403121560855760846041565b5b5f6090848285016061565b9150509291505056fea26469706673582212209d46704950c75b3505742b2236950b59adbcc17d023f976acb6b5c43bca401fc64736f6c634300081e0033").unwrap();

    chain.set_evm_bytecode(B160::from_be_bytes(inspector_addr.into_array()), &bytecode);
    chain.set_balance(
        B160::from_be_bytes(wallet.address().into_array()),
        U256::from(1_000_000_000_000_000_u64),
    );

    sol! {
        contract BlobHashEcho {
            function main(uint256 index) external returns (bytes32);
        }
    }

    // Test that blobhash instruction returns what we expect
    let versioned_hash =
        FixedBytes::from_hex("0x011122223333444455556666777788889999aaaabbbbccccddddeeeeffff0000")
            .unwrap();
    let calldata = BlobHashEcho::mainCall { index: U256::ZERO }.abi_encode();

    let tx = TxEip4844 {
        chain_id: 37u64,
        nonce: 0,
        max_fee_per_gas: 1_000,
        max_priority_fee_per_gas: 1_000,
        gas_limit: 300_000,
        to: inspector_addr,
        value: U256::ZERO,
        input: calldata.into(),
        access_list: Default::default(),
        blob_versioned_hashes: vec![versioned_hash],
        max_fee_per_blob_gas: 1_000,
    };
    let tx = rig::utils::sign_and_encode_alloy_tx(tx, &wallet);

    let run_config = rig::chain::RunConfig {
        app: Some("for_tests".to_string()),
        only_forward: false,
        check_storage_diff_hashes: true,
        ..Default::default()
    };
    let block_context = BlockContext {
        blob_fee: U256::from(1000),
        ..Default::default()
    };
    let result = chain.run_block(vec![tx], Some(block_context), Some(run_config));
    assert!(result.tx_results[0].is_ok(),);
    let tx_result = result.tx_results[0].as_ref().unwrap();
    assert!(tx_result.is_success(), "Transaction should be successful");
    // check the output to ensure the contract was called
    match &tx_result.execution_result {
        ExecutionResult::Success(ExecutionOutput::Call(out)) => {
            assert_eq!(out, &versioned_hash.0.to_vec(), "Output data doesn't match")
        }
        _ => panic!("Execution result must be a successful call"),
    }
}
