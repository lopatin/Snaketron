use super::traits::GameObjectRenderer;
use super::types::{CharDimensions, CharPattern};
use common::Direction;
use ratatui::style::{Color, Style};

pub struct StandardRenderer {
    char_dims: CharDimensions,
}

impl StandardRenderer {
    pub fn new(char_dims: CharDimensions) -> Self {
        Self { char_dims }
    }
}

impl GameObjectRenderer for StandardRenderer {
    fn char_dimensions(&self) -> CharDimensions {
        self.char_dims
    }

    fn render_snake_segment(
        &self,
        _direction: Option<Direction>,
        is_head: bool,
        player_id: u32,
        boost_active: bool,
    ) -> CharPattern {
        // A boosted snake uses a visibly different texture in addition to a
        // style modifier, so monochrome terminals still communicate state.
        let glyph = if boost_active { '▓' } else { '█' };
        let chars = vec![vec![glyph; self.char_dims.horizontal]; self.char_dims.vertical];

        // Select base color for the player
        let color = match player_id % 4 {
            0 => {
                if is_head {
                    Color::White
                } else {
                    Color::Gray
                }
            } // White → Gray
            1 => {
                if is_head {
                    Color::LightGreen
                } else {
                    Color::Green
                }
            } // Light Green → Green
            2 => {
                if is_head {
                    Color::LightBlue
                } else {
                    Color::Blue
                }
            } // Light Blue → Blue
            _ => {
                if is_head {
                    Color::LightYellow
                } else {
                    Color::Yellow
                }
            } // Light Yellow → Yellow
        };

        let style = if boost_active {
            Style::default()
                .fg(color)
                .bg(Color::DarkGray)
                .add_modifier(ratatui::style::Modifier::BOLD)
        } else {
            Style::default().fg(color)
        };
        CharPattern::new_with_style(chars, style)
    }

    fn render_food(&self) -> CharPattern {
        let chars = if self.char_dims.horizontal == 2 && self.char_dims.vertical == 1 {
            vec![vec!['●', '●']]
        } else if self.char_dims.horizontal == 1 && self.char_dims.vertical == 1 {
            vec![vec!['●']]
        } else {
            // For larger dimensions, create a pattern with food char in center-ish positions
            let mut pattern = vec![vec![' '; self.char_dims.horizontal]; self.char_dims.vertical];

            // Place food characters in a reasonable pattern
            for (y, row) in pattern.iter_mut().enumerate() {
                for (x, cell) in row.iter_mut().enumerate() {
                    // Create a checkerboard-like pattern for larger sizes
                    if (x + y) % 2 == 0 {
                        *cell = '●';
                    }
                }
            }
            pattern
        };

        // Food is always red
        let style = Style::default().fg(Color::Red);
        CharPattern::new_with_style(chars, style)
    }

    fn render_boost_pad(&self) -> CharPattern {
        // A filled, slanted canister silhouette is the terminal counterpart to
        // the blue NOS bottle used by the browser canvas. ArenaRenderer repeats
        // it over the authoritative footprint, so a full 2x2 pad remains four
        // times the area of a quarter 1x1 packet without retaining the legacy
        // pickup symbol in the alternate renderer.
        let glyph = '▰';
        let chars = vec![vec![glyph; self.char_dims.horizontal]; self.char_dims.vertical];
        let style = Style::default().fg(Color::LightBlue);
        CharPattern::new_with_style(chars, style)
    }

    fn render_empty(&self) -> CharPattern {
        CharPattern::empty(self.char_dims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boost_visuals_use_distinct_non_color_glyphs() {
        let renderer = StandardRenderer::new(CharDimensions::new(1, 1));
        let ordinary = renderer.render_snake_segment(None, false, 0, false);
        let boosted = renderer.render_snake_segment(None, false, 0, true);
        let available = renderer.render_boost_pad();

        assert_ne!(ordinary.chars, boosted.chars);
        assert_eq!(available.chars, vec![vec!['▰']]);
        assert_eq!(available.styles[0][0].fg, Some(Color::LightBlue));
    }
}
