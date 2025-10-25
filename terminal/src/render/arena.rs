use super::snake::SnakeRenderer;
use super::traits::GameObjectRenderer;
use super::types::{CharGrid, RenderConfig};
use common::Arena;

pub struct ArenaRenderer<R: GameObjectRenderer> {
    renderer: R,
}

impl<R: GameObjectRenderer> ArenaRenderer<R> {
    pub fn new(renderer: R) -> Self {
        Self { renderer }
    }

    pub fn render(&self, arena: &Arena, config: &RenderConfig) -> CharGrid {
        let mut grid = CharGrid::new(
            arena.width as usize,
            arena.height as usize,
            config.chars_per_point,
        );

        // Render food
        for food in &arena.food {
            if food.x >= 0
                && food.x < arena.width as i16
                && food.y >= 0
                && food.y < arena.height as i16
            {
                let pattern = self.renderer.render_food();
                grid.set_logical_point(food.x as usize, food.y as usize, &pattern);
            }
        }

        // Render snakes
        for (idx, snake) in arena.snakes.iter().enumerate() {
            if snake.is_alive {
                let positions = SnakeRenderer::expand_snake_body(snake);

                for (i, pos) in positions.iter().enumerate() {
                    if pos.x >= 0
                        && pos.x < arena.width as i16
                        && pos.y >= 0
                        && pos.y < arena.height as i16
                    {
                        let is_head = i == 0;
                        let direction = if is_head { Some(snake.direction) } else { None };
                        let pattern = self
                            .renderer
                            .render_snake_segment(direction, is_head, idx as u32);
                        grid.set_logical_point(pos.x as usize, pos.y as usize, &pattern);
                    }
                }
            }
        }

        grid
    }
}
