//! Manual native visual regression fixture: exercise eight independent real WebViews,
//! partially clipped documents and GPUI overlays. It is not application code.
use gpui_kit::{
    base::{h_flex, v_flex},
    component::{WindowExt, button::Button},
    prelude::*,
    *,
};
use gpui_webview::WebView;

struct Fixture {
    views: Vec<Entity<WebView>>,
}
impl Render for Fixture {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        let column = |range: std::ops::Range<usize>| {
            v_flex().flex_1().min_w_0().children(range.map(|i| {
                div()
                    .h(px(185.5))
                    .flex_shrink_0()
                    .overflow_hidden()
                    .child(self.views[i].clone())
            }))
        };
        v_flex()
            .size_full()
            .bg(rgb(0x161616))
            .text_color(rgb(0xeeeeee))
            .child(
                h_flex()
                    .h(px(42.))
                    .gap_4()
                    .items_center()
                    .child("Eight live HTML documents / native composition QA")
                    .child(
                        Button::new("overlay")
                            .label("Overlay")
                            .on_click(|_, window, cx| {
                                window.open_dialog(cx, |dialog, _, _| {
                                    dialog
                                        .title("Native overlay verification")
                                        .child("This GPUI dialog must cover the live WebViews.")
                                });
                            }),
                    ),
            )
            .child(
                h_flex()
                    .flex_1()
                    .min_h_0()
                    .overflow_hidden()
                    .child(column(0..4))
                    .child(column(4..8)),
            )
    }
}
fn main() {
    gpui_kit::application().with_assets(gpui_kit::assets::Assets).run(|cx| {
        gpui_kit::init(cx);
        gpui_kit::open_window(WindowOptions {
            window_bounds: Some(WindowBounds::Windowed(Bounds::new(point(px(50.),px(60.)),size(px(1285.),px(816.))))),
            ..Default::default()
        },cx,|window,cx| {
            let views=(0..8).map(|i| {
                let mut html=format!("<!doctype html><meta name=viewport content=\"width=device-width,initial-scale=1\"><style>html,body{{margin:0;width:100%;height:100%;overflow:hidden;background:#0a0a0a;color:#eee}}header{{height:28px;background:#323232}}main{{height:calc(100% - 28px);overflow:auto;display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:6px}}article{{min-height:50px;padding:6px;background:#173428}}</style><header id=counter>Document {i}</header><main>");
                for n in 0..600 { html.push_str(&format!("<article>{i}:{n} — wrapping text and responsive layout remain live</article>")); }
                html.push_str(&format!("</main><script>let n=0;function tick(){{counter.textContent='Document {i}, frame '+n++;requestAnimationFrame(tick)}}tick()</script>"));
                let view=WebView::build(wry::WebViewBuilder::new().with_html(html).with_incognito(true).with_visible(false),window,cx).unwrap();
                view.set_background_color((255,0,0,255)).unwrap();
                cx.new(|_|view)
            }).collect();
            cx.new(|_|Fixture { views })
        }).unwrap();
        cx.activate(true);
    });
}
