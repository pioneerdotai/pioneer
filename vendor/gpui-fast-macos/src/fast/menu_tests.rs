// Modified by Longbridge for gpui-fast.
//! The menu entry points are driven through Objective-C messages sent to a
//! test target whose methods forward to them exactly as the app delegate's do,
//! with real `NSMenuItem`s and, for validation, AppKit's own `-[NSMenu update]`
//! (the item enabling that `_simulateOpening:` also runs). Every test body runs
//! on its own thread under a timeout, so a deadlock fails the test instead of
//! hanging the run.

use super::{MenuState, handle_menu_item, menu_will_open, validate_menu_item};
use gpui::Action;
use objc2::{
    ClassType, class, msg_send,
    rc::{Retained, autoreleasepool},
    runtime::{AnyClass, AnyObject, Bool, ClassBuilder, Sel},
    sel,
};
use objc2_foundation::{NSInteger, NSObject};
use parking_lot::Mutex;
use std::{
    ffi::{CStr, c_void},
    sync::{OnceLock, mpsc},
    thread,
    time::Duration,
};

gpui::actions!(fast_menu_tests, [First, Second]);

#[derive(Default)]
struct TestState {
    menu_actions: Vec<Box<dyn Action>>,
    menu_command: Option<Box<dyn FnMut(&dyn Action)>>,
    validate_menu_command: Option<Box<dyn FnMut(&dyn Action) -> bool>>,
    will_open_menu: Option<Box<dyn FnMut()>>,
    log: Vec<String>,
    /// `validateMenuItem:` messages received, whatever their item.
    validations: usize,
}

impl MenuState for TestState {
    fn menu_actions(&self) -> &[Box<dyn Action>] {
        &self.menu_actions
    }

    fn menu_command(&mut self) -> &mut Option<Box<dyn FnMut(&dyn Action)>> {
        &mut self.menu_command
    }

    fn validate_menu_command(&mut self) -> &mut Option<Box<dyn FnMut(&dyn Action) -> bool>> {
        &mut self.validate_menu_command
    }

    fn will_open_menu(&mut self) -> &mut Option<Box<dyn FnMut()>> {
        &mut self.will_open_menu
    }
}

type State = &'static Mutex<TestState>;

const STATE_IVAR: &CStr = c"fastMenuTestState";

/// An `NSObject` subclass answering the app delegate's menu selectors through
/// the functions under test, with its state behind an ivar like the delegate's
/// platform pointer.
fn target_class() -> &'static AnyClass {
    struct Class(&'static AnyClass);
    // SAFETY: Objective-C classes are immutable once registered.
    unsafe impl Send for Class {}
    unsafe impl Sync for Class {}
    static CLASS: OnceLock<Class> = OnceLock::new();
    CLASS
        .get_or_init(|| {
            let mut builder = ClassBuilder::new(c"GPUIFastMenuTestTarget", NSObject::class())
                .expect("menu test target class");
            builder.add_ivar::<*mut c_void>(STATE_IVAR);
            unsafe {
                builder.add_method(
                    sel!(handleGPUIMenuItem:),
                    test_handle_menu_item as extern "C-unwind" fn(_, Sel, *mut AnyObject),
                );
                builder.add_method(
                    sel!(validateMenuItem:),
                    test_validate_menu_item as extern "C-unwind" fn(_, Sel, *mut AnyObject) -> Bool,
                );
                builder.add_method(
                    sel!(menuWillOpen:),
                    test_menu_will_open as extern "C-unwind" fn(_, Sel, *mut AnyObject),
                );
            }
            Class(builder.register())
        })
        .0
}

fn state_of(this: &AnyObject) -> State {
    unsafe {
        let ivar = this.class().instance_variable(STATE_IVAR).unwrap();
        &*(*ivar.load::<*mut c_void>(this) as *const Mutex<TestState>)
    }
}

extern "C-unwind" fn test_handle_menu_item(this: &AnyObject, _: Sel, item: *mut AnyObject) {
    unsafe { handle_menu_item(state_of(this), item) }
}

extern "C-unwind" fn test_validate_menu_item(
    this: &AnyObject,
    _: Sel,
    item: *mut AnyObject,
) -> Bool {
    state_of(this).lock().validations += 1;
    Bool::new(unsafe { validate_menu_item(state_of(this), item) })
}

extern "C-unwind" fn test_menu_will_open(this: &AnyObject, _: Sel, _: *mut AnyObject) {
    menu_will_open(state_of(this))
}

fn new_target(state: State) -> Retained<AnyObject> {
    unsafe {
        let target: Retained<AnyObject> = msg_send![target_class(), new];
        let ivar = target.class().instance_variable(STATE_IVAR).unwrap();
        *ivar.load_ptr::<*mut c_void>(&target) = state as *const _ as *mut c_void;
        target
    }
}

/// A menu item as AppKit creates it, with `tag` set unless `None` (default 0).
fn new_item(tag: Option<NSInteger>) -> Retained<AnyObject> {
    unsafe {
        let item: Retained<AnyObject> = msg_send![class!(NSMenuItem), new];
        if let Some(tag) = tag {
            let _: () = msg_send![&*item, setTag: tag];
        }
        item
    }
}

fn validate(target: &AnyObject, item: Option<&AnyObject>) -> bool {
    let item = item.map_or(std::ptr::null(), |item| item as *const AnyObject);
    let enabled: Bool = unsafe { msg_send![target, validateMenuItem: item] };
    enabled.as_bool()
}

fn handle(target: &AnyObject, item: &AnyObject) {
    let _: () = unsafe { msg_send![target, handleGPUIMenuItem: item] };
}

fn new_state(actions: Vec<Box<dyn Action>>) -> State {
    Box::leak(Box::new(Mutex::new(TestState {
        menu_actions: actions,
        ..Default::default()
    })))
}

/// A validation callback that logs the action and checks the lock is free.
fn logging_validator(state: State, enabled: bool) -> Box<dyn FnMut(&dyn Action) -> bool> {
    Box::new(move |action| {
        let mut state = state.try_lock().expect("validated with the lock held");
        state.log.push(format!("validate {}", action.name()));
        enabled
    })
}

fn logging_command(state: State) -> Box<dyn FnMut(&dyn Action)> {
    Box::new(move |action| {
        let mut state = state.try_lock().expect("dispatched with the lock held");
        state.log.push(format!("handle {}", action.name()));
    })
}

/// Runs `test` on a thread of its own and fails if it does not finish in time,
/// which is how a deadlock in the code under test shows up. A thread left
/// blocked ends with the test process.
fn within_timeout(test: impl FnOnce() + Send + 'static) {
    let (done_tx, done_rx) = mpsc::channel();
    let thread = thread::spawn(move || {
        autoreleasepool(|_| test());
        let _ = done_tx.send(());
    });
    match done_rx.recv_timeout(Duration::from_secs(5)) {
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("menu entry point deadlocked (no return within 5s)")
        }
        _ => {
            if let Err(panic) = thread.join() {
                std::panic::resume_unwind(panic);
            }
        }
    }
}

#[test]
fn unknown_tags_are_disabled_and_ignored_without_deadlocking() {
    within_timeout(|| {
        for actions in [vec![], vec![First.boxed_clone()]] {
            let action_count = actions.len() as NSInteger;
            let state = new_state(actions);
            state.lock().validate_menu_command = Some(logging_validator(state, true));
            state.lock().menu_command = Some(logging_command(state));
            let target = new_target(state);

            let mut items = vec![new_item(Some(action_count)), new_item(Some(-1))];
            items.push(new_item(Some(NSInteger::MAX)));
            items.push(new_item(Some(NSInteger::MIN)));
            if action_count == 0 {
                // An AppKit-built item keeps the default tag 0.
                items.push(new_item(None));
            }
            for item in &items {
                assert!(!validate(&target, Some(item)));
                handle(&target, item);
            }
            if action_count == 0 {
                assert!(!validate(&target, None));
            }

            let state = state.lock();
            assert!(state.log.is_empty(), "{:?}", state.log);
            assert!(state.validate_menu_command.is_some());
            assert!(state.menu_command.is_some());
        }
    });
}

#[test]
fn known_tags_reach_the_callbacks_and_restore_them() {
    within_timeout(|| {
        let state = new_state(vec![First.boxed_clone(), Second.boxed_clone()]);
        let target = new_target(state);
        let first = new_item(None);
        let second = new_item(Some(1));

        // Without callbacks nothing is enabled or dispatched.
        assert!(!validate(&target, Some(&first)));
        handle(&target, &second);

        state.lock().validate_menu_command = Some(logging_validator(state, true));
        state.lock().menu_command = Some(logging_command(state));
        for _ in 0..2 {
            assert!(validate(&target, Some(&first)));
            assert!(validate(&target, None), "a null item stands for tag 0");
            assert!(validate(&target, Some(&second)));
            handle(&target, &second);
        }
        state.lock().validate_menu_command = Some(logging_validator(state, false));
        assert!(!validate(&target, Some(&first)));

        let state = state.lock();
        let validate_first = "validate fast_menu_tests::First";
        let validate_second = "validate fast_menu_tests::Second";
        let handle_second = "handle fast_menu_tests::Second";
        let round = [
            validate_first,
            validate_first,
            validate_second,
            handle_second,
        ];
        let mut expected = [round, round].concat();
        expected.push(validate_first);
        assert_eq!(state.log, expected);
    });
}

#[test]
fn callbacks_replaced_while_running_keep_the_replacement() {
    within_timeout(|| {
        let state = new_state(vec![First.boxed_clone()]);
        let target = new_target(state);
        let item = new_item(Some(0));

        state.lock().validate_menu_command = Some(Box::new(move |_| {
            state.lock().validate_menu_command = Some(logging_validator(state, false));
            true
        }));
        state.lock().menu_command = Some(Box::new(move |_| {
            state.lock().menu_command = Some(logging_command(state));
        }));
        state.lock().will_open_menu = Some(Box::new(move || {
            state.lock().will_open_menu = Some(Box::new(move || {
                state.lock().log.push("replacement opened".into());
            }));
        }));

        assert!(validate(&target, Some(&item)));
        assert!(!validate(&target, Some(&item)));
        handle(&target, &item);
        handle(&target, &item);
        for _ in 0..2 {
            let _: () = unsafe { msg_send![&*target, menuWillOpen: std::ptr::null::<AnyObject>()] };
        }

        assert_eq!(
            state.lock().log,
            [
                "validate fast_menu_tests::First",
                "handle fast_menu_tests::First",
                "replacement opened",
            ]
        );
    });
}

/// Each callback in turn sends all three menu messages while it runs. Its own
/// message finds the callback taken (a nested validation is disabled, a nested
/// dispatch or menu-open dropped, like upstream); the others reach theirs.
#[test]
fn reentrant_menu_messages_do_not_deadlock() {
    within_timeout(|| {
        for reentrant in ["validate", "handle", "open"] {
            let state = new_state(vec![First.boxed_clone()]);
            let target = new_target(state);
            let target_ptr = Retained::as_ptr(&target) as usize;
            let reenter = move || {
                let target = unsafe { &*(target_ptr as *const AnyObject) };
                let known = new_item(Some(0));
                let unknown = new_item(Some(7));
                let nested = [
                    validate(target, Some(&known)),
                    validate(target, Some(&unknown)),
                ];
                handle(target, &known);
                handle(target, &unknown);
                let _: () =
                    unsafe { msg_send![target, menuWillOpen: std::ptr::null::<AnyObject>()] };
                state
                    .lock()
                    .log
                    .push(format!("{reentrant} nested validations {nested:?}"));
            };

            let mut guard = state.lock();
            guard.validate_menu_command = Some(logging_validator(state, true));
            guard.menu_command = Some(logging_command(state));
            guard.will_open_menu = Some(Box::new(move || {
                state.lock().log.push("opened".into());
            }));
            match reentrant {
                "validate" => {
                    guard.validate_menu_command = Some(Box::new(move |_| {
                        reenter();
                        true
                    }))
                }
                "handle" => guard.menu_command = Some(Box::new(move |_| reenter())),
                _ => guard.will_open_menu = Some(Box::new(reenter)),
            }
            drop(guard);

            let item = new_item(None);
            match reentrant {
                "validate" => assert!(validate(&target, Some(&item))),
                "handle" => handle(&target, &item),
                _ => {
                    let _: () =
                        unsafe { msg_send![&*target, menuWillOpen: std::ptr::null::<AnyObject>()] };
                }
            }

            let state = state.lock();
            let validated = "validate fast_menu_tests::First";
            let handled = "handle fast_menu_tests::First";
            let expected = match reentrant {
                "validate" => [
                    handled,
                    "opened",
                    "validate nested validations [false, false]",
                ],
                "handle" => [
                    validated,
                    "opened",
                    "handle nested validations [true, false]",
                ],
                _ => [validated, handled, "open nested validations [true, false]"],
            };
            assert_eq!(state.log, expected);
            assert!(state.validate_menu_command.is_some());
            assert!(state.menu_command.is_some());
            assert!(state.will_open_menu.is_some());
        }
    });
}

#[test]
fn menu_will_open_runs_and_restores_the_callback() {
    within_timeout(|| {
        let state = new_state(Vec::new());
        let target = new_target(state);
        let open = || {
            let _: () = unsafe { msg_send![&*target, menuWillOpen: std::ptr::null::<AnyObject>()] };
        };
        open();
        state.lock().will_open_menu = Some(Box::new(move || {
            let mut state = state.try_lock().expect("opened with the lock held");
            state.log.push("opened".into());
        }));
        open();
        open();
        assert_eq!(state.lock().log, ["opened", "opened"]);
        assert!(state.lock().will_open_menu.is_some());
    });
}

/// AppKit's own item enabling, which the system's periodic menu harvesting
/// (`-[NSMenu _simulateOpening:]`) also runs: an `NSMenu` built with AppKit,
/// whose nil-target items reach the app delegate through the responder chain,
/// updated repeatedly from a background thread. The first item keeps AppKit's
/// default tag 0, the others run past the action table, which is empty in the
/// issue's case.
#[test]
fn appkit_menu_update_validates_through_the_app_delegate() {
    within_timeout(|| unsafe {
        // AppKit only enables menu items once the application object exists.
        // It is not run, activated or given a window, so nothing is shown.
        let app: *mut AnyObject = msg_send![class!(NSApplication), sharedApplication];
        let app = &*app;
        for actions in [Vec::new(), vec![First.boxed_clone(), Second.boxed_clone()]] {
            let action_count = actions.len() as NSInteger;
            let state = new_state(actions);
            state.lock().validate_menu_command = Some(Box::new(move |action| {
                let mut state = state.try_lock().expect("validated with the lock held");
                state.log.push(format!("validate {}", action.name()));
                action.name() == "fast_menu_tests::First"
            }));
            let target = new_target(state);

            let menu: Retained<AnyObject> = msg_send![class!(NSMenu), new];
            let items: Vec<_> = (0..action_count.max(1) + 2)
                .map(|tag| {
                    let item = new_item((tag > 0).then_some(tag));
                    let _: () = msg_send![&*item, setAction: sel!(handleGPUIMenuItem:)];
                    let _: () = msg_send![&*menu, addItem: &*item];
                    item
                })
                .collect();
            let autoenables: Bool = msg_send![&*menu, autoenablesItems];
            assert!(autoenables.as_bool());

            let previous_delegate: *mut AnyObject = msg_send![app, delegate];
            let _: () = msg_send![app, setDelegate: &*target];
            for _ in 0..20 {
                let _: () = msg_send![&*menu, update];
            }
            let _: () = msg_send![app, setDelegate: previous_delegate];

            let enabled: Vec<bool> = items
                .iter()
                .map(|item| {
                    let enabled: Bool = msg_send![&**item, isEnabled];
                    enabled.as_bool()
                })
                .collect();
            let mut expected = vec![false; items.len()];
            expected[0] = action_count > 0;
            assert_eq!(enabled, expected);

            let state = state.lock();
            assert!(
                state.validations >= 20 * items.len(),
                "AppKit validated through the delegate {} times",
                state.validations
            );
            if action_count == 0 {
                assert!(state.log.is_empty(), "{:?}", state.log);
            } else {
                let validated = |name: &str| state.log.iter().filter(|l| l.ends_with(name)).count();
                assert_eq!(validated("::First"), 20);
                assert_eq!(validated("::Second"), 20);
                assert_eq!(state.log.len(), 40);
            }
            assert!(state.validate_menu_command.is_some());
        }
    });
}
