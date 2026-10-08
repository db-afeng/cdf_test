# Control centre design

This app is a small engineering console. A presenter deletes source orders, runs
independent pipelines and compares the results. Put these controls and comparisons
on the first screen.

## Layout

- Compact page title and one sentence explaining the task.
- Source namespace and last refresh time identify the observations.
- A single control strip groups the delete input, run-both action and reset.
- One comparison table aligns source/A/B row counts, new events and pending events.
- Selected orders appear first. Extra sample orders are opt-in.
- Keep count explanations and historical events in expandable sections.
- Use spacing and table rules to group content; no nested cards or decorative badges.

## Typography

Native system sans-serif for the interface. Use ui-monospace, SFMono-Regular or
Consolas for dataset names. Numeric columns use tabular figures and right alignment.
Sizes: 13px secondary text, 14px tables/controls, 16px body, 20px section headings,
30px page title (26px on narrow screens). Body line height is 1.5. Avoid uppercase
eyebrows, artificial letter spacing and oversized display text.

## Palette and shape

White background; #f5f7f9 grouped control/source surfaces; #1d2933 primary text;
#52616b secondary text; #d7dfe4 table rules. #194f90 indicates actions and links;
#a62c34 indicates deletion; #216343 indicates present records; #80540c indicates
pending changes. Color accompanies readable status text. Use a 4px corner radius
for controls and notices; tables are flat. No gradients, shadows or decorative motion.

## States and copy

Show loading, empty, error and stale states inline. Keep the last observations on a
refresh error and label them as stale. Disable mutations during a run or an active
refresh. Provide visible keyboard focus, a skip link, table column headers and
accessible scroll regions. Respect reduced-motion settings.

Use the humanize skill's default voice profile: short, specific copy and natural
contractions, without slogans, manufactured fragments or repeated instructions.
The Impeccable slop guidance informs the compact heading, flat comparison layout,
readable secondary text, semantic colors and removal of redundant copy.
