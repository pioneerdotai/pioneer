// Fake coverage of the production worker/supervisor boundary. These tests do
// not assert ONNX equivalence or claim to execute a real acoustic model.
mod engine_sessions {
    use super::super::super::{
        runtime::LoadedVoiceEngine,
        supervisor::{
            EagerVoiceEngineLoader, FilesystemVoiceModelInstaller, VoiceInputDesiredState,
            VoiceModelIdentity,
        },
    };
    use super::*;
    use pioneer_protocol::GatewayVoiceInputProvider;

    #[derive(Default)]
    struct Trace {
        segments: Vec<Vec<f32>>,
        finishes: usize,
        previews: usize,
        one_shots: usize,
        dropped: usize,
    }

    struct FakeNative {
        trace: Arc<Mutex<Trace>>,
        index: usize,
        stop: DecodingCompletion,
        block: Option<Arc<Gate>>,
    }
    impl Drop for FakeNative {
        fn drop(&mut self) {
            self.trace.lock().unwrap().dropped += 1;
        }
    }
    impl VoiceNativeSession for FakeNative {
        fn feed(&mut self, samples: &[f32]) -> Result<(), VoiceTranscriptionError> {
            if let Some(gate) = self.block.take() {
                gate.entered.send(()).unwrap();
                gate.release.lock().unwrap().recv().unwrap();
            }
            let mut trace = self.trace.lock().unwrap();
            let segment = &mut trace.segments[self.index];
            if segment.len() + samples.len() > 18 * 16000 {
                return Err(transcription_error(
                    VoiceTranscriptionErrorKind::RuntimeFailure,
                    "segment too large",
                ));
            }
            segment.extend_from_slice(samples);
            Ok(())
        }
        fn hypothesis(&mut self) -> Result<VoiceSpeechHypothesis, VoiceTranscriptionError> {
            self.trace.lock().unwrap().previews += 1;
            // Intentionally different from final output. Runtime must retain
            // tentative status even when a preview reached EOS.
            Ok(VoiceSpeechHypothesis {
                completion: DecodingCompletion::Normal,
                ..VoiceSpeechHypothesis::text_only("tentative must not escape".to_owned())
            })
        }
        fn finish(&mut self) -> Result<VoiceSpeechHypothesis, VoiceTranscriptionError> {
            self.trace.lock().unwrap().finishes += 1;
            Ok(VoiceSpeechHypothesis {
                completion: self.stop,
                ..VoiceSpeechHypothesis::text_only(format!("segment{}", self.index))
            })
        }
    }

    struct Gate {
        entered: tokio::sync::mpsc::UnboundedSender<()>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
    }
    // Always unblock on test failure, so blocking worker shutdown cannot hang.
    struct Release(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for Release {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    fn ready(engine: LoadedVoiceEngine) -> SessionSpeechTranscriber {
        let supervisor = Arc::new(VoiceInputSupervisor::new(
            Arc::new(FilesystemVoiceModelInstaller::new(
                crate::isolated_test_app_config().unwrap(),
                std::path::PathBuf::new(),
            )),
            Arc::new(EagerVoiceEngineLoader),
        ));
        // Deliberately use a non-streaming model ID: dispatch must use the
        // actual loaded adapter API, never the catalog name/streaming flag.
        let identity = VoiceModelIdentity {
            provider: GatewayVoiceInputProvider::Local,
            model: "small".to_owned(),
        };
        let generation = supervisor
            .apply_desired(
                VoiceInputDesiredState {
                    enabled: true,
                    provider: Some(identity.provider),
                    model: Some(identity.model.clone()),
                },
                false,
            )
            .unwrap()
            .generation;
        supervisor.mark_loading(generation).unwrap();
        supervisor.mark_ready(generation, identity, engine).unwrap();
        SessionSpeechTranscriber {
            supervisor,
            generation,
            is_moonshine_base: false,
            cancellation: CancellationToken::new(),
            fake: None,
        }
    }

    fn engine(
        trace: &Arc<Mutex<Trace>>,
        stop_from_segment: Option<(usize, DecodingCompletion)>,
        block: Option<Arc<Gate>>,
    ) -> LoadedVoiceEngine {
        let for_batch = trace.clone();
        let for_native = trace.clone();
        LoadedVoiceEngine::TestNative {
            one_shot: Arc::new(move |_| {
                for_batch.lock().unwrap().one_shots += 1;
                Ok("batch".to_owned())
            }),
            begin: Arc::new(move || {
                let mut trace = for_native.lock().unwrap();
                let index = trace.segments.len();
                trace.segments.push(Vec::new());
                drop(trace);
                Box::new(FakeNative {
                    trace: for_native.clone(),
                    index,
                    block: block.clone(),
                    stop: stop_from_segment
                        .filter(|(first, _)| index >= *first)
                        .map(|(_, stop)| stop)
                        .unwrap_or(DecodingCompletion::Normal),
                })
            }),
        }
    }

    #[test]
    fn native_dispatch_preserves_order_final_tail_bounds_and_recording_isolation() {
        let trace = Arc::new(Mutex::new(Trace::default()));
        let transcriber = ready(engine(&trace, None, None));
        let audio: Vec<i16> = (0..2 * 18 * 16000 + 137)
            .map(|i| 8000 + (i % 4000) as i16)
            .collect();
        let result = closed_input(&audio, 317, false, &transcriber).unwrap();
        let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result else {
            panic!("transcript expected")
        };
        assert_eq!(transcript.text, "segment0 segment1 segment2");
        assert_eq!(transcript.diagnostics.speech_samples, audio.len());
        assert_eq!(transcript.diagnostics.segment_count, 3);
        let expected: Vec<f32> = audio.iter().map(|s| *s as f32 / 32768.0).collect();
        {
            let trace = trace.lock().unwrap();
            assert_eq!(
                trace.segments.iter().map(Vec::len).collect::<Vec<_>>(),
                [288000, 288000, 137]
            );
            assert_eq!(trace.segments.concat(), expected);
            assert_eq!(trace.one_shots, 0);
            assert_eq!(trace.finishes, 3);
            assert_eq!(trace.dropped, 3);
        }
        // Same loaded engine, another recording. Its first feed owns only its
        // PCM; there is no leftover session or acoustic prefix from recording 1.
        let result = closed_input(&vec![12000; 1600], 73, false, &transcriber).unwrap();
        let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result else {
            panic!("transcript expected")
        };
        assert_eq!(transcript.text, "segment3");
        assert_eq!(
            trace.lock().unwrap().segments[3],
            vec![12000.0 / 32768.0; 1600]
        );
    }

    #[test]
    fn native_segments_finish_through_existing_pause_endpoint() {
        let trace = Arc::new(Mutex::new(Trace::default()));
        let transcriber = ready(engine(&trace, None, None));
        let mut audio = vec![12000; 16000];
        audio.extend(vec![0; 3 * 16000]);
        audio.extend(vec![12000; 1600]);
        let result = closed_input(&audio, 317, false, &transcriber).unwrap();
        let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result else {
            panic!("transcript expected")
        };
        assert_eq!(transcript.text, "segment0 segment1");
        let trace = trace.lock().unwrap();
        assert_eq!(trace.segments.len(), 2);
        assert_eq!(trace.finishes, 2);
        assert_eq!(trace.one_shots, 0);
    }

    #[test]
    fn buffered_never_begins_native_and_result_only_engine_keeps_one_shot_path() {
        let trace = Arc::new(Mutex::new(Trace::default()));
        let native = ready(engine(&trace, None, None));
        let result =
            closed_input_mode(&vec![12000; 20 * 16000 + 137], 317, false, &native, true).unwrap();
        let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result else {
            panic!("batch expected")
        };
        assert_eq!(transcript.text, "batch");
        let trace = trace.lock().unwrap();
        assert_eq!(trace.one_shots, 1);
        assert!(trace.segments.is_empty());
        drop(trace);
        let calls = Arc::new(Mutex::new(0));
        let captured = calls.clone();
        let one_shot = ready(LoadedVoiceEngine::TestStub(Arc::new(move |_| {
            *captured.lock().unwrap() += 1;
            Ok("unknown backend".to_owned())
        })));
        let result = closed_input(&vec![12000; 1600], 317, false, &one_shot).unwrap();
        let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } = result else {
            panic!("one-shot expected")
        };
        assert_eq!(transcript.text, "unknown backend");
        assert_eq!(*calls.lock().unwrap(), 1);
    }

    #[test]
    fn completion_rejections_never_publish_a_previous_segment_or_preview() {
        for stop in [
            DecodingCompletion::BudgetExhausted,
            DecodingCompletion::RepetitionGuard,
            DecodingCompletion::Unknown,
        ] {
            let trace = Arc::new(Mutex::new(Trace::default()));
            let transcriber = ready(engine(&trace, Some((1, stop)), None));
            let error = closed_input(&vec![12000; 18 * 16000 + 1600], 317, false, &transcriber)
                .err()
                .expect("incomplete final segment must fail");
            assert_eq!(error.kind, VoiceErrorKind::TranscriptionFailed);
            let trace = trace.lock().unwrap();
            assert_eq!(trace.finishes, 2); // first segment really succeeded
            assert_eq!(trace.one_shots, 0); // no full-recording fallback
        }
        // Result-only backends accept Unknown. Explicit known stops cannot be
        // accepted in buffered or one-shot incremental terminal paths either.
        for buffered in [false, true] {
            for stop in [
                DecodingCompletion::Normal,
                DecodingCompletion::Unknown,
                DecodingCompletion::BudgetExhausted,
                DecodingCompletion::RepetitionGuard,
            ] {
                let transcriber = ready(LoadedVoiceEngine::TestHypothesis(VoiceSpeechHypothesis {
                    completion: stop,
                    ..VoiceSpeechHypothesis::text_only("nonempty prefix".to_owned())
                }));
                let result =
                    closed_input_mode(&vec![12000; 1600], 317, false, &transcriber, buffered);
                assert_eq!(
                    result.is_ok(),
                    matches!(
                        stop,
                        DecodingCompletion::Normal | DecodingCompletion::Unknown
                    )
                );
            }
        }
    }

    #[test]
    fn failed_native_feed_closes_recording_without_one_shot_retry() {
        struct BrokenNative;
        impl VoiceNativeSession for BrokenNative {
            fn feed(&mut self, _: &[f32]) -> Result<(), VoiceTranscriptionError> {
                Err(transcription_error(
                    VoiceTranscriptionErrorKind::RuntimeFailure,
                    "native feed failed",
                ))
            }
            fn hypothesis(&mut self) -> Result<VoiceSpeechHypothesis, VoiceTranscriptionError> {
                panic!("failed native feed must terminate")
            }
            fn finish(&mut self) -> Result<VoiceSpeechHypothesis, VoiceTranscriptionError> {
                panic!("failed native feed must terminate")
            }
        }
        let begins = Arc::new(Mutex::new(0));
        let captured = begins.clone();
        let transcriber = ready(LoadedVoiceEngine::TestNative {
            one_shot: Arc::new(|_| panic!("native failure must not fall back")),
            begin: Arc::new(move || {
                *captured.lock().unwrap() += 1;
                Box::new(BrokenNative)
            }),
        });
        let result = closed_input(&vec![12000; 1600], 317, false, &transcriber);
        assert_eq!(
            result.err().expect("failed recording").kind,
            VoiceErrorKind::TranscriptionFailed
        );
        assert_eq!(*begins.lock().unwrap(), 1);
        assert!(!transcriber.supervisor.has_loaded_engine());
    }

    #[test]
    fn preview_eos_stays_tentative_and_replaced_generations_cannot_feed_old_state() {
        let trace = Arc::new(Mutex::new(Trace::default()));
        let mut transcriber = ready(engine(&trace, None, None));
        let mut session = transcriber.begin_native_session().unwrap().unwrap();
        session.feed(&[0.5; 1280]).unwrap();
        assert!(session.hypothesis().unwrap().ensure_complete().is_err());
        // Idle state owns no engine mutex: an unrelated one-shot can complete.
        let buffer = PreparedSpeechBuffer {
            sample_rate_hz: 16000,
            total_samples: 1280,
            segments: vec![VoiceSpeechSegment {
                start_sample: 0,
                end_sample: 1280,
                samples: vec![0.5; 1280],
            }],
        };
        assert_eq!(transcriber.supervisor.transcribe(&buffer).unwrap(), "batch");
        let identity = VoiceModelIdentity {
            provider: GatewayVoiceInputProvider::Local,
            model: "small".to_owned(),
        };
        let desired = transcriber.supervisor.desired();
        let generation = transcriber
            .supervisor
            .apply_desired(desired, true)
            .unwrap()
            .generation;
        transcriber.supervisor.mark_loading(generation).unwrap();
        transcriber
            .supervisor
            .mark_ready(generation, identity, engine(&trace, None, None))
            .unwrap();
        assert!(session.feed(&[0.25; 480]).is_err());
        assert!(session.finish().is_err());
        transcriber.generation = generation;
        let mut fresh = transcriber.begin_native_session().unwrap().unwrap();
        fresh.feed(&[0.25; 480]).unwrap();
        assert_eq!(fresh.finish().unwrap().text, "segment1");
        assert!(fresh.finish().is_err()); // exactly one terminal native operation
        let trace = trace.lock().unwrap();
        assert_eq!(trace.segments, [vec![0.5; 1280], vec![0.25; 480]]);
        assert_eq!(trace.finishes, 1);
    }

    #[tokio::test]
    async fn blocked_native_retains_lease_through_cancel_disconnect_reload_and_shutdown() {
        for action in ["cancel", "disconnect", "reload", "shutdown", "success"] {
            let trace = Arc::new(Mutex::new(Trace::default()));
            let (entered, mut receive) = tokio::sync::mpsc::unbounded_channel();
            let (release, released) = std::sync::mpsc::channel();
            let release = Release(Some(release));
            let gate = Arc::new(Gate {
                entered,
                release: Mutex::new(released),
            });
            let mut transcriber = ready(engine(&trace, None, Some(gate)));
            let store = GatewayVoiceSessionBufferStore::default();
            let workers = VoiceFinalizations::default();
            transcriber.cancellation = workers.shutdown.child_token();
            let cancellation = transcriber.cancellation.clone();
            let supervisor = transcriber.supervisor.clone();
            start_with_detector(
                store.clone(),
                "session".to_owned(),
                VoiceAudioFormat::pioneer_streaming_target(),
                &workers,
                workers.reserve(1).unwrap(),
                transcriber,
                true,
                |_| Ok(test_detector()),
            )
            .unwrap();
            append_test_packet(&store, "session", 0, &[12000; 1600]);
            let mut completion = store.finish_processing("session").unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(5), receive.recv())
                .await
                .unwrap()
                .unwrap();
            // The barrier is inside feed with the actual supervisor mutex held;
            // cancellation is issued only after native entry, never beforehand.
            assert!(workers.reserve(1).is_none());
            let reload = match action {
                "cancel" => {
                    cancellation.cancel();
                    None
                }
                "disconnect" => {
                    store.remove_session("session").unwrap();
                    None
                }
                "shutdown" => {
                    workers.close();
                    store.cancel_all();
                    None
                }
                "reload" => {
                    // Reload is serialized behind the native operation. Fence
                    // the recording as real model replacement ingress does.
                    cancellation.cancel();
                    Some(tokio::task::spawn_blocking(move || {
                        supervisor
                            .apply_desired(VoiceInputDesiredState::default(), false)
                            .unwrap()
                    }))
                }
                _ => None,
            };
            assert!(workers.reserve(1).is_none());
            assert!(matches!(
                completion.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
            drop(release);
            let result = tokio::time::timeout(std::time::Duration::from_secs(5), &mut completion)
                .await
                .unwrap()
                .unwrap();
            if action == "success" {
                let GatewayVoiceSessionPipelineOutcome::Transcript { transcript, .. } =
                    result.outcome.unwrap()
                else {
                    panic!("final transcript expected")
                };
                assert_eq!(transcript.text, "segment0");
                assert_eq!(trace.lock().unwrap().finishes, 1);
            } else {
                assert_eq!(
                    result.outcome.err().expect("cancel must fail closed").kind,
                    VoiceErrorKind::Cancelled
                );
                assert_eq!(trace.lock().unwrap().finishes, 0);
            }
            // Completion publication still owns the admission lease.
            assert!(workers.reserve(1).is_none());
            drop(result.lease);
            if let Some(reload) = reload {
                reload.await.unwrap();
            }
            workers.tasks.close();
            tokio::time::timeout(std::time::Duration::from_secs(5), workers.tasks.wait())
                .await
                .unwrap();
            assert!(!workers.has_active_work());
            assert_eq!(trace.lock().unwrap().dropped, 1);
            assert_eq!(trace.lock().unwrap().one_shots, 0);
        }
    }
}
