use common::{Arena, Snake, Position, Direction};
use terminal::render::{
    arena::ArenaRenderer,
    standard_renderer::StandardRenderer,
    types::{CharDimensions, RenderConfig},
};

fn main() {
    // Create a demo arena with multiple snakes
    let arena = Arena {
        width: 20,
        height: 10,
        snakes: vec![
            Snake {
                body: vec![
                    Position { x: 5, y: 5 },   // head
                    Position { x: 5, y: 7 },   // turn point
                    Position { x: 3, y: 7 },   // tail
                ],
                direction: Direction::Up,
                is_alive: true,
                food: 0,
            },
            Snake {
                body: vec![
                    Position { x: 15, y: 3 },  // head
                    Position { x: 12, y: 3 },  // tail
                ],
                direction: Direction::Right,
                is_alive: true,
                food: 0,
            },
        ],
        food: vec![
            Position { x: 10, y: 5 },
            Position { x: 2, y: 2 },
            Position { x: 17, y: 8 },
        ],
    };

    println!("=== 1x1 Rendering (Classic) ===");
    render_with_dimensions(&arena, CharDimensions::new(1, 1));
    
    println!("\n=== 2x1 Rendering (Wide) ===");
    render_with_dimensions(&arena, CharDimensions::new(2, 1));
    
    println!("\n=== 3x2 Rendering (Large) ===");
    render_with_dimensions(&arena, CharDimensions::new(3, 2));
}

fn render_with_dimensions(arena: &Arena, char_dims: CharDimensions) {
    let renderer = StandardRenderer::new(char_dims);
    let arena_renderer = ArenaRenderer::new(renderer);
    let config = RenderConfig { chars_per_point: char_dims };
    
    let char_grid = arena_renderer.render(arena, &config);
    let lines = char_grid.into_lines();
    
    println!("Dimensions: {}x{} chars per point", char_dims.horizontal, char_dims.vertical);
    println!("Physical size: {}x{} characters", 
        arena.width as usize * char_dims.horizontal,
        arena.height as usize * char_dims.vertical
    );
    
    // Draw top border
    print!("┌");
    for _ in 0..arena.width as usize * char_dims.horizontal {
        print!("─");
    }
    println!("┐");
    
    // Draw arena with side borders
    for line in lines {
        print!("│");
        for ch in line {
            print!("{}", ch);
        }
        println!("│");
    }
    
    // Draw bottom border
    print!("└");
    for _ in 0..arena.width as usize * char_dims.horizontal {
        print!("─");
    }
    println!("┘");
}