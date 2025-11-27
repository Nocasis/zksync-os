use crypto::ark_ff::{One, PrimeField, Zero};

///  The number of field elements in a blob.
const FIELD_ELEMENTS_PER_BLOB: usize = 4096;

/// The number of field elements in an extended blob
const FIELD_ELEMENTS_PER_EXT_BLOB: usize = 8192;

// taken from https://github.com/ethereum/c-kzg-4844/blob/c88a50d633cc0703d8da7a1f21264e65ce6e3062/src/setup/setup.c#L49
/**
 * This is the root of unity associated with FIELD_ELEMENTS_PER_EXT_BLOB.
 *
 * Compute this constant with the scripts below:
 *
 * @code{.py}
 * import math
 *
 * FIELD_ELEMENTS_PER_EXT_BLOB = 8192
 * PRIMITIVE_ROOT_OF_UNITY = 7
 * BLS_MODULUS = 52435875175126190479447740508185965837690552500527637822603658699938581184513
 *
 * order = int(math.log2(FIELD_ELEMENTS_PER_EXT_BLOB))
 * root_of_unity = pow(PRIMITIVE_ROOT_OF_UNITY, (BLS_MODULUS - 1) // (2**order), BLS_MODULUS)
 * uint64s = [(root_of_unity >> (64 * i)) & 0xFFFFFFFFFFFFFFFF for i in range(4)]
 * values = [f"0x{uint64:016x}L" for uint64 in uint64s]
 * print(f"{{{', '.join(values)}}}")
 * @endcode
**/
const ROOT_OF_UNITY: crypto::BigInt<4> = crypto::BigInt([
    0x6fdd00bfc78c8967,
    0x146b58bc434906ac,
    0x2ccddea2972e89ed,
    0x485d512737b1da3d,
]);

// not really efficient, but it shouldn't be
///
/// Calculates roots of unity and then generates rust code which defines constant array with first `FIELD_ELEMENTS_PER_BLOB`(4192) brp roots of unity.
/// The result is printed to stdout.
///
pub fn generate_brp_roots_of_unity_const_to_std() {
    let mut roots_of_unity = [crypto::bls12_381::Fr::zero(); FIELD_ELEMENTS_PER_EXT_BLOB];
    roots_of_unity[0] = crypto::bls12_381::Fr::one();
    roots_of_unity[1] = crypto::bls12_381::Fr::from_bigint(ROOT_OF_UNITY).unwrap();
    for i in 2..FIELD_ELEMENTS_PER_EXT_BLOB {
        roots_of_unity[i] = roots_of_unity[i - 1] * roots_of_unity[1];
        if roots_of_unity[i].is_one() {
            break;
        }
    }
    fn log2_pow2(mut n: u64) -> u64 {
        let mut position = 0;
        n >>= 1;
        while n > 0 {
            position += 1;
            n >>= 1;
        }
        position
    }

    fn reverse_bits(mut n: u64) -> u64 {
        let mut result = 0;
        for _ in 0..64 {
            result <<= 1;
            result |= n & 1;
            n >>= 1;
        }
        result
    }
    let unused_bit_len = 64 - log2_pow2(FIELD_ELEMENTS_PER_EXT_BLOB as u64);
    for i in 0..FIELD_ELEMENTS_PER_EXT_BLOB {
        let r = (reverse_bits(i as u64) >> unused_bit_len) as usize;
        if r > i {
            roots_of_unity.swap(i, r);
        }
    }
    println!("pub const BRP_ROOTS_OF_UNITY: [crypto::bls12_381::Fr; ELEMENTS_PER_4844_BLOB] = [");
    for el in roots_of_unity.iter().take(FIELD_ELEMENTS_PER_BLOB) {
        println!("    crypto::bls12_381::Fp(");
        println!("        crypto::BigInt([");
        println!("            {},", el.0 .0[0]);
        println!("            {},", el.0 .0[1]);
        println!("            {},", el.0 .0[2]);
        println!("            {},", el.0 .0[3]);
        println!("        ]),");
        println!("        core::marker::PhantomData,");
        println!("    ),");
    }
    println!("];");
}

fn main() {
    generate_brp_roots_of_unity_const_to_std();
}
