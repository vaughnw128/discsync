// built-in
use std::path::Path;
use std::sync::Arc;

// external
use anyhow::{anyhow, Result};
use ndarray::{Array2, IxDyn, Axis};
use ndarray::CowArray;
use ort::{Environment, Session, SessionBuilder, Value};
use ort::tensor::OrtOwnedTensor;
use tokenizers::Tokenizer;
use ndarray::Array3;
use tracing::info;
use ort::GraphOptimizationLevel;

pub const VECTOR_DIM: usize = 384;

pub struct Embedder {
    _env: Arc<Environment>,
    session: Session,
    tokenizer: Tokenizer,
}

/// Build a local ONNX embedder (downloads model/tokenizer on first run).
pub async fn new_embedder(model_dir: &Path) -> Result<Embedder> {
    let (env, session, tokenizer) = load_embedder(model_dir).await?;
    Ok(Embedder { _env: env, session, tokenizer })
}

impl Embedder {
    pub async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        embed_batch_local(&self.session, &self.tokenizer, texts)
    }
    pub fn dim(&self) -> usize { VECTOR_DIM }
}

/// Loads the ONNX model and tokenizer locally (downloads once if missing).
pub async fn load_embedder(model_dir: &Path) -> Result<(Arc<Environment>, Session, Tokenizer)> {
    tokio::fs::create_dir_all(model_dir).await.ok();

    let model_path = model_dir.join("model.onnx");
    let tok_path = model_dir.join("tokenizer.json");

    if !model_path.exists() {
        download_to(
            "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/onnx/model.onnx",
            &model_path,
        )
        .await?;
    }
    if !tok_path.exists() {
        download_to(
            "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/tokenizer.json",
            &tok_path,
        )
        .await?;
    }

    let env = Arc::new(Environment::builder().with_name("sbux").build()?);
    let session = SessionBuilder::new(&env)?
        .with_optimization_level(GraphOptimizationLevel::Level3)?
        .with_intra_threads(4)?
        .with_model_from_file(&model_path)?;

    info!("ONNX session initialized (always using 3 inputs: input_ids, attention_mask, token_type_ids)");

    let tokenizer = Tokenizer::from_file(&tok_path)
        .map_err(|e| anyhow!("failed to load tokenizer: {e}"))?;

    Ok((env, session, tokenizer))
}

/// Simple async file downloader used for first-run model/tokenizer fetch.
async fn download_to(url: &str, path: &Path) -> Result<()> {
    let bytes = reqwest::get(url).await?.bytes().await?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    tokio::fs::write(path, &bytes).await?;
    Ok(())
}

// ==== Refactored helpers for readability ====

/// Simple container for model input tensors (int64) with shape [batch, seq_len].
struct ModelInputs {
    input_ids: Array2<i64>,
    attention_mask: Array2<i64>,
    token_type_ids: Array2<i64>,
}

/// Tokenize a batch of texts and build padded/truncated arrays up to max_len_cap.
fn prepare_model_inputs(tokenizer: &Tokenizer, texts: &[&str], max_len_cap: usize) -> Result<ModelInputs> {
    // Encode with special tokens, let tokenizer decide per-example length
    let encodings = tokenizer
        .encode_batch(texts.iter().map(|s| s.to_string()).collect::<Vec<_>>(), true)
        .map_err(|e| anyhow!("tokenization failed: {e}"))?;

    let batch = encodings.len();
    let max_len = encodings.iter().map(|e| e.len()).max().unwrap_or(0).min(max_len_cap);
    if batch == 0 || max_len == 0 {
        // Return empty arrays; caller handles early return.
        return Ok(ModelInputs {
            input_ids: Array2::zeros((0, 0)),
            attention_mask: Array2::zeros((0, 0)),
            token_type_ids: Array2::zeros((0, 0)),
        });
    }

    // Flatten and pad to [batch, max_len]
    let input_ids_flat: Vec<i64> = encodings.iter().flat_map(|e| {
        let ids = e.get_ids();
        let take = ids.len().min(max_len);
        ids[..take]
            .iter()
            .map(|&v| v as i64)
            .chain(std::iter::repeat(0).take(max_len - take))
    }).collect();

    let attention_mask_flat: Vec<i64> = encodings.iter().flat_map(|e| {
        let mask = e.get_attention_mask();
        let take = mask.len().min(max_len);
        mask[..take]
            .iter()
            .map(|&v| v as i64)
            .chain(std::iter::repeat(0).take(max_len - take))
    }).collect();

    let token_type_ids_flat: Vec<i64> = encodings.iter().flat_map(|e| {
        let types = e.get_type_ids();
        if types.is_empty() {
            std::iter::repeat(0).take(max_len).collect::<Vec<i64>>()
        } else {
            let take = types.len().min(max_len);
            types[..take]
                .iter()
                .map(|&v| v as i64)
                .chain(std::iter::repeat(0).take(max_len - take))
                .collect::<Vec<i64>>()
        }
    }).collect();

    Ok(ModelInputs {
        input_ids: Array2::<i64>::from_shape_vec((batch, max_len), input_ids_flat)
            .map_err(|e| anyhow!("failed to shape input_ids: {e}"))?,
        attention_mask: Array2::<i64>::from_shape_vec((batch, max_len), attention_mask_flat)
            .map_err(|e| anyhow!("failed to shape attention_mask: {e}"))?,
        token_type_ids: Array2::<i64>::from_shape_vec((batch, max_len), token_type_ids_flat)
            .map_err(|e| anyhow!("failed to shape token_type_ids: {e}"))?,
    })
}

/// Run the ONNX model and return the last hidden states with shape [batch, seq_len, hidden].
fn run_model(session: &Session, inputs: &ModelInputs) -> Result<Array3<f32>> {
    // Create owned dynamic arrays so values can safely borrow their memory
    let input_ids_owned = inputs.input_ids.clone().into_dyn();
    let attention_mask_owned = inputs.attention_mask.clone().into_dyn();
    let token_type_ids_owned = inputs.token_type_ids.clone().into_dyn();

    // Prepare ONNX values using owned array backing
    let input_ids_cow: CowArray<'_, i64, IxDyn> = CowArray::from(input_ids_owned);
    let attention_mask_cow: CowArray<'_, i64, IxDyn> = CowArray::from(attention_mask_owned);
    let token_type_ids_cow: CowArray<'_, i64, IxDyn> = CowArray::from(token_type_ids_owned);

    let onnx_inputs = vec![
        Value::from_array(std::ptr::null_mut(), &input_ids_cow)?,
        Value::from_array(std::ptr::null_mut(), &attention_mask_cow)?,
        Value::from_array(std::ptr::null_mut(), &token_type_ids_cow)?,
    ];

    let outputs = session.run(onnx_inputs)?;

    // Extract and coerce to [b, s, h]
    let tensor: OrtOwnedTensor<f32, _> = outputs[0].try_extract()?;
    let view = tensor.view();
    let out3 = view
        .to_owned()
        .into_dimensionality::<ndarray::Ix3>()
        .map_err(|_| anyhow!("unexpected ONNX output shape: {:?}", view.shape()))?;
    Ok(out3)
}

/// Mean-pool over the sequence axis using the attention mask (ignoring PAD tokens).
fn mean_pool(hidden: &Array3<f32>, attention_mask: &Array2<i64>) -> Array2<f32> {
    let mask_f = attention_mask.mapv(|x| x as f32);              // [b, s]
    let masked = hidden * &mask_f.view().insert_axis(Axis(2));   // [b, s, h]
    let sums = masked.sum_axis(Axis(1));                         // [b, h]
    let counts = mask_f
        .sum_axis(Axis(1))
        .insert_axis(Axis(1))                                    // [b, 1]
        .mapv(|c| if c <= 0.0 { 1.0 } else { c });               // avoid div by 0
    &sums / &counts                                              // [b, h]
}

/// L2-normalize each row vector; guards against tiny norms.
fn l2_normalize_rows(x: Array2<f32>) -> Array2<f32> {
    let norms = x
        .mapv(|v| v * v)
        .sum_axis(Axis(1))
        .mapv(|s| s.sqrt().max(1e-12))
        .insert_axis(Axis(1));
    &x / &norms
}

/// Convert to Vec<Vec<f32>>, truncating/padding to target_dim if different.
fn to_vecs_with_dim(x: &Array2<f32>, target_dim: usize) -> Vec<Vec<f32>> {
    let b = x.nrows();
    let h = x.ncols();
    let mut result: Vec<Vec<f32>> = Vec::with_capacity(b);
    if h == target_dim {
        result = x.outer_iter().map(|row| row.to_vec()).collect();
    } else {
        for row in x.outer_iter() {
            let mut v = row.to_vec();
            if h > target_dim { v.truncate(target_dim); }
            if h < target_dim { v.resize(target_dim, 0.0); }
            result.push(v);
        }
    }
    result
}

/// Local ONNX embedding implementation.
fn embed_batch_local(session: &Session, tokenizer: &Tokenizer, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
    if texts.is_empty() { return Ok(Vec::new()); }

    // 1) Tokenize + pad
    let inputs = prepare_model_inputs(tokenizer, texts, 256)?;
    if inputs.input_ids.is_empty() { return Ok(Vec::new()); }

    // 2) Run ONNX model -> [b, s, h]
    let hidden = run_model(session, &inputs)?;

    // 3) Mean-pool with mask -> [b, h]
    let pooled = mean_pool(&hidden, &inputs.attention_mask);

    // 4) Normalize -> [b, h]
    let normalized = l2_normalize_rows(pooled);

    // 5) Collect and conform to VECTOR_DIM
    Ok(to_vecs_with_dim(&normalized, VECTOR_DIM))
}
