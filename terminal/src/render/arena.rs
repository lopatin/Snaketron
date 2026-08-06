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

        // Available pads render every authoritative footprint cell. A full
        // packet therefore occupies 2x2 logical cells while a quarter packet
        // occupies 1x1. Cooling pads remain in state but intentionally render
        // nothing.
        for pad in &arena.boost_pads {
            if pad.respawn_at_tick.is_some() {
                continue;
            }
            for cell in pad.footprint_cells() {
                if cell.x >= 0
                    && cell.x < arena.width as i16
                    && cell.y >= 0
                    && cell.y < arena.height as i16
                {
                    let pattern = self.renderer.render_boost_pad();
                    grid.set_logical_point(cell.x as usize, cell.y as usize, &pattern);
                }
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
                        let pattern = self.renderer.render_snake_segment(
                            direction,
                            is_head,
                            idx as u32,
                            snake.boost().active,
                        );
                        grid.set_logical_point(pos.x as usize, pos.y as usize, &pattern);
                    }
                }
            }
        }

        grid
    }
}
