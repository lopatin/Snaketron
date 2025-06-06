use common::Direction;
use ratatui::style::{Color, Style};
use super::traits::GameObjectRenderer;
use super::types::{CharPattern, CharDimensions};

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
    ) -> CharPattern {
        // Use solid blocks for all snakes, vary only the color
        let chars = vec![vec!['█'; self.char_dims.horizontal]; self.char_dims.vertical];
        
        // Select base color for the player
        let color = match player_id % 4 {
            0 => if is_head { Color::White } else { Color::Gray },          // White → Gray
            1 => if is_head { Color::LightGreen } else { Color::Green },    // Light Green → Green
            2 => if is_head { Color::LightBlue } else { Color::Blue },      // Light Blue → Blue
            _ => if is_head { Color::LightYellow } else { Color::Yellow },  // Light Yellow → Yellow
        };
        
        let style = Style::default().fg(color);
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
            for y in 0..self.char_dims.vertical {
                for x in 0..self.char_dims.horizontal {
                    // Create a checkerboard-like pattern for larger sizes
                    if (x + y) % 2 == 0 {
                        pattern[y][x] = '●';
                    }
                }
            }
            pattern
        };
        
        // Food is always red
        let style = Style::default().fg(Color::Red);
        CharPattern::new_with_style(chars, style)
    }
    
    fn render_empty(&self) -> CharPattern {
        CharPattern::empty(self.char_dims)
    }
}