use arrow::{
    array::{
        make_array, Array, ArrayData, ArrayIter, Float64Array, Int64Array, RecordBatch, StringArray,
    },
    datatypes::{ArrowNativeType, DataType, Field, Schema},
    pyarrow::PyArrowType,
};
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use rand::seq::SliceRandom;
use rand::{
    Rng, SeedableRng,
};
use rand::distributions::{Distribution, Standard, Uniform};
use rand_chacha::ChaCha8Rng;
use std::sync::Arc;

/// Parameters of transactions amount distribution
const MIN: f64 = 100.0;
const MAX: f64 = 10000.0;

/// Parameters of transactions amount distribution
/// We will sample in a way like from V * BOTTOM / 100 to V * UPPER / 100
const BOTTOM: i64 = 90;
const UPPER: i64 = 110;

/// We have two kinds of cards, debit and credit
/// We will choose DS with probability 0.75 and CC otherwise
const CARD_TYPES: (&str, &str) = ("DC", "CC");

/// We have different transactions categories
/// We will choose them uniformly
/// Inspired by: https://medium.com/@swedbank.tech/how-does-categorization-of-transactions-work-4262d720fd2d
const TRANSACTION_TYPES: [&str; 13] = [
    "food-and-household",
    "home",
    "uncategorized",
    "leisure-and-lifestyle",
    "health-and-beauty",
    "shopping-and-services",
    "children",
    "vacation-and-travel",
    "education",
    "insurance",
    "investments-and-savings",
    "expenses-and-other",
    "cars-and-transportation",
];

/// We have different channels
/// We will choose mobile with probability 0.25 classic otherwise
const CHANNELS: (&str, &str) = ("mobile", "web");


/// Result of intermediate generation
struct TransactionsResult {
    customer_id: Vec<i64>,
    card_type: Vec<&'static str>,
    trx_type: Vec<&'static str>,
    channel: Vec<&'static str>,
    trx_amnt: Vec<f64>,
    t_minus: Vec<i64>,
}

/// Generate data for a single customer;
/// Returns expected_amnt of rows in a fixed schema.
fn generate_customer_transactions(
    id: i64,
    expected_amnt: i64,
    distr_trx: Uniform<f64>,
    seed: u64,
    d_minus: i64,
) -> TransactionsResult {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    // We will sample from expected amount;
    // Interval is defined on the top of the file
    let distr_90p =
        Uniform::<i64>::try_from((expected_amnt * BOTTOM / 100)..(expected_amnt * UPPER / 100))
            .unwrap();
    let sampled_expected = distr_90p.sample(&mut rng).as_usize();

    // I will store rows as columns in form of vectors
    let ids: Vec<i64> = vec![id; sampled_expected];
    let mut card_t: Vec<&'static str> = Vec::with_capacity(sampled_expected);
    let mut trx_t: Vec<&'static str> = Vec::with_capacity(sampled_expected);
    let mut ch_t: Vec<&'static str> = Vec::with_capacity(sampled_expected);
    let mut amnt: Vec<f64> = Vec::with_capacity(sampled_expected);
    let t_minus: Vec<i64> = vec![d_minus; sampled_expected];

    for _i in 0..sampled_expected {
        // Choose DC with probability 75%
        let card_type = if rng.gen_bool(0.75) {
            CARD_TYPES.0
        } else {
            CARD_TYPES.1
        };

        // Choose random transaction type
        let trx_type = TRANSACTION_TYPES.choose(&mut rng).unwrap();

        // Choose mobile channel with probability 25%
        let channel_type = if rng.gen_bool(0.25) {
            CHANNELS.0
        } else {
            CHANNELS.1
        };

        // Sample from uniform distribution for an amount
        let trx_amnt = distr_trx.sample(&mut rng);

        card_t.push(card_type);
        trx_t.push(trx_type);
        ch_t.push(channel_type);
        amnt.push(trx_amnt);
    }

    TransactionsResult {
        customer_id: ids,
        card_type: card_t,
        trx_type: trx_t,
        channel: ch_t,
        trx_amnt: amnt,
        t_minus,
    }
}

/// Generate batch of random seeded transactions for the given customers list considering expected amount of transactions
#[pyfunction]
fn generate_data_batch(
    ids: PyArrowType<ArrayData>,
    trx_per_day: PyArrowType<ArrayData>,
    days_in_batch: i64,
    offset: i64,
    global_seed: u64,
    partition_name: &str,
) -> PyArrowType<RecordBatch> {
    let ids_arr = make_array(ids.0);
    let expected_arr = make_array(trx_per_day.0);
    let dim = ids_arr.len() * days_in_batch.as_usize();

    let trx_amnt_distr = Uniform::<f64>::try_from(MIN..MAX).unwrap();

    // I will store generated batches as vector of vectors
    let mut results_of_generation: Vec<TransactionsResult> = Vec::with_capacity(dim);

    // This generator is used for generating of u64 seeds for mini-batch generation
    let mut global_rnd = ChaCha8Rng::seed_from_u64(global_seed);

    let iter = ArrayIter::new(
        ids_arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| PyRuntimeError::new_err("Expected int64 ids"))
            .unwrap(),
    )
    .zip(ArrayIter::new(
        expected_arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| PyRuntimeError::new_err("Expected int64 amounts"))
            .unwrap(),
    ));
    let mut local_offset;

    // Outer loop over pairs (customer, expected_amnt)
    for (id, expected) in iter {
        local_offset = offset;
        // Inner loop over requested batch length;
        // Here we are simulating length in days
        for _i in 0..days_in_batch {
            let generated_part = generate_customer_transactions(
                id.unwrap(),
                expected.unwrap(),
                trx_amnt_distr,
                global_rnd.sample(Standard),
                local_offset,
            );
            results_of_generation.push(generated_part)
        }
    }

    // Our intermediate results are in form Vec<Vec<T>>;
    // But we need them in form of Arrow structures.
    // I do the following:
    // 1. flatten vec
    // 2. *x to convert from &T to T
    // 3. invoke from constructor of arrow arrays
    let ids_res: Int64Array = Int64Array::from(
        results_of_generation
            .iter()
            .map(|x| x.customer_id.iter())
            .flatten()
            .map(|x| *x)
            .collect::<Vec<i64>>(),
    );
    let card_t_res = StringArray::from(
        results_of_generation
            .iter()
            .map(|x| x.card_type.iter())
            .flatten()
            .map(|x| *x)
            .collect::<Vec<&str>>(),
    );
    let trx_t_res = StringArray::from(
        results_of_generation
            .iter()
            .map(|x| x.trx_type.iter())
            .flatten()
            .map(|x| *x)
            .collect::<Vec<&str>>(),
    );
    let ch_t_res = StringArray::from(
        results_of_generation
            .iter()
            .map(|x| x.channel.iter())
            .flatten()
            .map(|x| *x)
            .collect::<Vec<&str>>(),
    );
    let trx_res = Float64Array::from(
        results_of_generation
            .iter()
            .map(|x| x.trx_amnt.iter())
            .flatten()
            .map(|x| *x)
            .collect::<Vec<f64>>(),
    );
    let t_minus_res = Int64Array::from(
        results_of_generation
            .iter()
            .map(|x| x.t_minus.iter())
            .flatten()
            .map(|x| *x)
            .collect::<Vec<i64>>(),
    );

    // Partition column is a constant; may it be done more efficiently?
    let part_col = StringArray::try_from(vec![partition_name; t_minus_res.len()]).unwrap();

    // Output schema of the dataset
    let schema = Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("card_type", DataType::Utf8, false),
        Field::new("trx_type", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("trx_amnt", DataType::Float64, false),
        Field::new("t_minus", DataType::Int64, false),
        Field::new("part_col", DataType::Utf8, false), // partition column
    ]);

    // Just put everything together
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(ids_res),
            Arc::new(card_t_res),
            Arc::new(trx_t_res),
            Arc::new(ch_t_res),
            Arc::new(trx_res),
            Arc::new(t_minus_res),
            Arc::new(part_col),
        ],
    )
    .unwrap();

    // And wrap to pyarrow
    PyArrowType(batch)
}

#[pymodule]
fn native(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(generate_data_batch, m)?)?;
    Ok(())
}
