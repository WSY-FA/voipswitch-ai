use ai_protocol::id::ProviderId;
use ai_provider::{ByteDanceTtsConfig, ByteDanceTtsProvider, TtsProvider, TtsRequest};

#[tokio::test]
#[ignore = "requires explicit BD_APP_ID, BD_TOKEN, BD_CLUSTER, and BD_VOICE_TYPE"]
async fn live_bytedance_tts_smoke() {
    let app_id = std::env::var_os("BD_APP_ID").expect("BD_APP_ID");
    let token = std::env::var("BD_TOKEN").expect("BD_TOKEN");
    let provider = ByteDanceTtsProvider::new(
        ProviderId::new("live-bytedance-tts").unwrap(),
        ByteDanceTtsConfig {
            endpoint: "https://openspeech.bytedance.com/api/v1/tts".to_string(),
            app_id: app_id.to_string_lossy().into_owned(),
            cluster: std::env::var("BD_CLUSTER").expect("BD_CLUSTER"),
            voice_type: std::env::var("BD_VOICE_TYPE").expect("BD_VOICE_TYPE"),
            uid: "voipswitch-live-test".to_string(),
            language: "zh".to_string(),
            sample_rate: 16_000,
            request_timeout_seconds: 30,
            enabled: true,
        },
        token,
    )
    .unwrap();
    let output = provider
        .synthesize(TtsRequest {
            operation_id: "voipswitch-live-tts-test".to_string(),
            text: "您好，这是 VoIPSwitch AI-03 语音合成测试。".to_string(),
            voice: String::new(),
        })
        .await
        .unwrap();
    assert!(!output.pcm16_le.is_empty());
    assert_eq!(output.sample_rate, 16_000);
    println!(
        "BYTEDANCE_TTS_OK sample_rate={} pcm_bytes={}",
        output.sample_rate,
        output.pcm16_le.len()
    );
}
