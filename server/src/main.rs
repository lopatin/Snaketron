use common::*;

fn main() {
    let x = add(2, 2);
    println!("2 + 2 = {}", x);

    let direction = Direction::Up;

    let snake = Snake {
        body: vec![(0, 0), (0, 1), (0, 2)],
        direction,
    };

    println!("Snake head: {:?}", snake.head());
    println!("Snake direction: {:?}", snake.direction);
}
