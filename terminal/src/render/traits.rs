use super::types::{CharDimensions, CharPattern};
use common::Direction;

pub trait GameObjectRenderer {
    fn char_dimensions(&self) -> CharDimensions;

    fn render_snake_segment(
        &self,
        direction: Option<Direction>,
        is_head: bool,
        player_id: u32,
    ) -> CharPattern;

    fn render_food(&self) -> CharPattern;

    fn render_empty(&self) -> CharPattern;
}
