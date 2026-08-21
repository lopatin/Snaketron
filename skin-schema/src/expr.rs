//! The skin expression language.
//!
//! Total and non-Turing-complete: no loops, no recursion, no user-defined
//! functions, no unbounded anything. Every expression terminates in time
//! proportional to its own size, and its size is bounded at parse time.
//!
//! `specs/skin-shading-prd.md` section 9.2 is explicit that this totality is a
//! **sandbox boundary, not a convenience cap**. First-party skins are the only
//! ones that ship today, so nothing here is currently defending against a
//! hostile author — but relaxing it for first-party ergonomics is exactly what
//! would have to be undone if user submissions ever return, and by then there
//! would be skins depending on the relaxation. So it does not get relaxed.
//!
//! The evaluation tier is **derived, not declared**. An author writes what they
//! mean and the compiler works out how often it has to be evaluated:
//!
//! | Tier | Uses | Cost |
//! | --- | --- | --- |
//! | [`Tier::Constant`] | none of `s`, `t`, `time`, `noise` | folded at registration |
//! | [`Tier::PerStep`] | `time` | 32 values, baked |
//! | [`Tier::PerCell`] | `s` | evaluated in the existing cell walk |
//! | [`Tier::PerTexel`] | `t` or `noise` | baked into a tile |
//!
//! Declaring the tier instead would let an author claim a `noise(s, t)` field
//! is constant, and the first thing that would notice is the frame rate.

use std::fmt;

/// How often an expression has to be evaluated.
///
/// Ordered cheapest first, so combining two subexpressions is a `max`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Tier {
    Constant,
    PerStep,
    PerCell,
    PerTexel,
}

/// A value an expression can read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Input {
    /// Arc length from the head, in cells.
    S,
    /// Across the body, `-0.5..0.5`.
    T,
    /// The body's total length in cells.
    Len,
    /// Position in the animation cycle, `0..1` turns.
    Time,
    /// `1` while boosting, `0` otherwise.
    Boost,
    /// Stable per snake, so two snakes wearing one skin can differ.
    Seed,
}

impl Input {
    /// Every input, for exhaustive reasoning about what an expression may read.
    pub const ALL: [Input; 6] = [
        Input::S,
        Input::T,
        Input::Len,
        Input::Time,
        Input::Boost,
        Input::Seed,
    ];

    /// The name an author writes.
    pub fn name(self) -> &'static str {
        match self {
            Input::S => "s",
            Input::T => "t",
            Input::Len => "len",
            Input::Time => "time",
            Input::Boost => "boost",
            Input::Seed => "seed",
        }
    }

    fn bit(self) -> u8 {
        match self {
            Input::S => 1,
            Input::T => 1 << 1,
            Input::Len => 1 << 2,
            Input::Time => 1 << 3,
            Input::Boost => 1 << 4,
            Input::Seed => 1 << 5,
        }
    }

    fn tier(self) -> Tier {
        match self {
            Input::S => Tier::PerCell,
            Input::T => Tier::PerTexel,
            Input::Time => Tier::PerStep,
            // `len`, `boost` and `seed` are fixed for a whole snake-frame, so
            // they cost nothing beyond what the frame already knows.
            Input::Len | Input::Boost | Input::Seed => Tier::Constant,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Func {
    Sin,
    Cos,
    Saw,
    Tri,
    Pulse,
    Fract,
    Floor,
    Abs,
    Clamp,
    Mix,
    Smoothstep,
    Step,
    Min,
    Max,
    Noise,
}

impl Func {
    /// Complete callable vocabulary, in the stable order exposed to authors.
    pub const ALL: [Self; 15] = [
        Self::Sin,
        Self::Cos,
        Self::Saw,
        Self::Tri,
        Self::Pulse,
        Self::Fract,
        Self::Floor,
        Self::Abs,
        Self::Clamp,
        Self::Mix,
        Self::Smoothstep,
        Self::Step,
        Self::Min,
        Self::Max,
        Self::Noise,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Func::Sin => "sin",
            Func::Cos => "cos",
            Func::Saw => "saw",
            Func::Tri => "tri",
            Func::Pulse => "pulse",
            Func::Fract => "fract",
            Func::Floor => "floor",
            Func::Abs => "abs",
            Func::Clamp => "clamp",
            Func::Mix => "mix",
            Func::Smoothstep => "smoothstep",
            Func::Step => "step",
            Func::Min => "min",
            Func::Max => "max",
            Func::Noise => "noise",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "sin" => Func::Sin,
            "cos" => Func::Cos,
            "saw" => Func::Saw,
            "tri" => Func::Tri,
            "pulse" => Func::Pulse,
            "fract" => Func::Fract,
            "floor" => Func::Floor,
            "abs" => Func::Abs,
            "clamp" => Func::Clamp,
            "mix" => Func::Mix,
            "smoothstep" => Func::Smoothstep,
            "step" => Func::Step,
            "min" => Func::Min,
            "max" => Func::Max,
            "noise" => Func::Noise,
            _ => return None,
        })
    }

    /// Accepted argument counts. `pulse` takes an optional duty cycle.
    fn arity(self) -> &'static [usize] {
        match self {
            Func::Sin
            | Func::Cos
            | Func::Saw
            | Func::Tri
            | Func::Fract
            | Func::Floor
            | Func::Abs => &[1],
            Func::Pulse => &[1, 2],
            Func::Step | Func::Min | Func::Max | Func::Noise => &[2],
            Func::Clamp | Func::Mix | Func::Smoothstep => &[3],
        }
    }
}

/// A set of [`Input`]s, as a bitset.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Inputs(u8);

impl Inputs {
    pub fn contains(self, input: Input) -> bool {
        self.0 & input.bit() != 0
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The first input in this set that is not in `allowed`.
    ///
    /// Returns the offender rather than a bool so an error can name it: "reads
    /// `boost`" is actionable where "reads something it may not" is not.
    pub fn first_outside(self, allowed: &[Input]) -> Option<Input> {
        Input::ALL
            .into_iter()
            .find(|input| self.contains(*input) && !allowed.contains(input))
    }

    pub fn iter(self) -> impl Iterator<Item = Input> {
        Input::ALL.into_iter().filter(move |i| self.contains(*i))
    }
}

/// A parsed expression.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Const(f64),
    Input(Input),
    Neg(Box<Expr>),
    Bin(BinOp, Box<Expr>, Box<Expr>),
    Call(Func, Vec<Expr>),
}

/// Everything an expression can read.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Env {
    pub s: f64,
    pub t: f64,
    pub len: f64,
    /// `0..1` turns through the animation cycle.
    pub time: f64,
    pub boost: f64,
    pub seed: f64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExprError {
    pub problem: String,
    /// Byte offset into the source, for a caret in an authoring tool.
    pub at: usize,
}

impl fmt::Display for ExprError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} (at byte {})", self.problem, self.at)
    }
}

/// How deeply an expression may nest.
///
/// This is the totality guarantee made concrete. Evaluation recurses over the
/// tree, so an unbounded tree would be an unbounded stack — and a parser with
/// no depth cap turns a long string of `(((((…` into a crash rather than an
/// error message. Fifty is far past anything legible.
const MAX_DEPTH: usize = 50;

impl Expr {
    /// Parse an expression, or say precisely what is wrong with it.
    pub fn parse(source: &str) -> Result<Self, ExprError> {
        let tokens = tokenize(source)?;
        let mut parser = Parser {
            tokens: &tokens,
            position: 0,
            depth: 0,
        };
        let expr = parser.expression(0)?;
        if parser.position < parser.tokens.len() {
            return Err(ExprError {
                problem: "trailing input after a complete expression".to_string(),
                at: parser.tokens[parser.position].at,
            });
        }
        Ok(expr)
    }

    /// Every input this expression reads.
    ///
    /// The tier answers "how often does this have to be evaluated"; this
    /// answers "what does it depend on", and the two are different questions.
    /// `boost` and `seed` are constant-tier — they never change within one
    /// snake-frame — yet an expression reading them cannot be folded at
    /// registration, because it is not constant across *snakes*. A compiler
    /// that only consulted the tier would freeze a boost-reactive layer at
    /// "not boosting" and nothing would report it.
    pub fn inputs(&self) -> Inputs {
        match self {
            Expr::Const(_) => Inputs::default(),
            Expr::Input(input) => Inputs(input.bit()),
            Expr::Neg(inner) => inner.inputs(),
            Expr::Bin(_, left, right) => Inputs(left.inputs().0 | right.inputs().0),
            Expr::Call(_, args) => Inputs(args.iter().fold(0, |bits, arg| bits | arg.inputs().0)),
        }
    }

    /// How often this expression has to be evaluated.
    pub fn tier(&self) -> Tier {
        match self {
            Expr::Const(_) => Tier::Constant,
            Expr::Input(input) => input.tier(),
            Expr::Neg(inner) => inner.tier(),
            Expr::Bin(_, left, right) => left.tier().max(right.tier()),
            // Noise is per-texel however it is called: it is the one function
            // whose whole purpose is to vary faster than its arguments suggest.
            Expr::Call(Func::Noise, args) => {
                args.iter().map(Expr::tier).fold(Tier::PerTexel, Tier::max)
            }
            Expr::Call(_, args) => args.iter().map(Expr::tier).fold(Tier::Constant, Tier::max),
        }
    }

    /// Evaluate. Always returns a finite number.
    ///
    /// Division by zero yields `0` rather than an infinity, because the result
    /// of one of these usually ends up in a colour channel or an opacity, and a
    /// NaN there propagates into a canvas call that silently paints nothing.
    /// A wrong pixel is debuggable; an invisible snake is not.
    pub fn eval(&self, env: &Env) -> f64 {
        let value = self.eval_raw(env);
        if value.is_finite() { value } else { 0.0 }
    }

    fn eval_raw(&self, env: &Env) -> f64 {
        match self {
            Expr::Const(value) => *value,
            Expr::Input(input) => match input {
                Input::S => env.s,
                Input::T => env.t,
                Input::Len => env.len,
                Input::Time => env.time,
                Input::Boost => env.boost,
                Input::Seed => env.seed,
            },
            Expr::Neg(inner) => -inner.eval(env),
            Expr::Bin(op, left, right) => {
                let (left, right) = (left.eval(env), right.eval(env));
                match op {
                    BinOp::Add => left + right,
                    BinOp::Sub => left - right,
                    BinOp::Mul => left * right,
                    BinOp::Div => {
                        if right == 0.0 {
                            0.0
                        } else {
                            left / right
                        }
                    }
                }
            }
            Expr::Call(func, args) => {
                let arg = |index: usize| args.get(index).map_or(0.0, |expr| expr.eval(env));
                match func {
                    Func::Sin => arg(0).sin(),
                    Func::Cos => arg(0).cos(),
                    Func::Saw => fract(arg(0)),
                    Func::Tri => {
                        let phase = fract(arg(0));
                        1.0 - (2.0 * phase - 1.0).abs()
                    }
                    Func::Pulse => {
                        let duty = if args.len() > 1 { arg(1) } else { 0.5 };
                        if fract(arg(0)) < duty { 1.0 } else { 0.0 }
                    }
                    Func::Fract => fract(arg(0)),
                    Func::Floor => arg(0).floor(),
                    Func::Abs => arg(0).abs(),
                    Func::Clamp => {
                        let (value, low, high) = (arg(0), arg(1), arg(2));
                        // A reversed range is an author error, not a reason to
                        // return something surprising.
                        value.clamp(low.min(high), low.max(high))
                    }
                    Func::Mix => {
                        let (a, b, amount) = (arg(0), arg(1), arg(2));
                        a + (b - a) * amount
                    }
                    Func::Smoothstep => {
                        let (edge0, edge1, x) = (arg(0), arg(1), arg(2));
                        if edge0 == edge1 {
                            return if x < edge0 { 0.0 } else { 1.0 };
                        }
                        let t = ((x - edge0) / (edge1 - edge0)).clamp(0.0, 1.0);
                        t * t * (3.0 - 2.0 * t)
                    }
                    Func::Step => {
                        if arg(1) < arg(0) {
                            0.0
                        } else {
                            1.0
                        }
                    }
                    Func::Min => arg(0).min(arg(1)),
                    Func::Max => arg(0).max(arg(1)),
                    Func::Noise => value_noise(arg(0), arg(1)),
                }
            }
        }
    }
}

fn fract(value: f64) -> f64 {
    value - value.floor()
}

/// Deterministic value noise on a unit lattice, smoothed.
///
/// Hash-based rather than table-based so it needs no allocation and no
/// initialisation, and identical on every platform: the same skin must look the
/// same for everyone in a match, and a noise field that differed per machine
/// would be a cosmetic desync.
fn value_noise(x: f64, y: f64) -> f64 {
    fn hash(xi: i64, yi: i64) -> f64 {
        let mut h = (xi as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        h ^= (yi as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
        h ^= h >> 29;
        h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        h ^= h >> 32;
        (h >> 11) as f64 / (1u64 << 53) as f64
    }

    let (x0, y0) = (x.floor(), y.floor());
    let (fx, fy) = (x - x0, y - y0);
    // Smoothstep the interpolation so the lattice does not show as a grid.
    let (ux, uy) = (fx * fx * (3.0 - 2.0 * fx), fy * fy * (3.0 - 2.0 * fy));
    let (xi, yi) = (x0 as i64, y0 as i64);

    let top = hash(xi, yi) + (hash(xi + 1, yi) - hash(xi, yi)) * ux;
    let bottom = hash(xi, yi + 1) + (hash(xi + 1, yi + 1) - hash(xi, yi + 1)) * ux;
    top + (bottom - top) * uy
}

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Number(f64),
    Name(String),
    Plus,
    Minus,
    Star,
    Slash,
    Open,
    Close,
    Comma,
}

#[derive(Clone, Debug)]
struct Spanned {
    token: Token,
    at: usize,
}

fn tokenize(source: &str) -> Result<Vec<Spanned>, ExprError> {
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0usize;

    while index < bytes.len() {
        let byte = bytes[index];
        let at = index;
        match byte {
            b' ' | b'\t' | b'\n' | b'\r' => index += 1,
            b'+' => {
                tokens.push(Spanned {
                    token: Token::Plus,
                    at,
                });
                index += 1;
            }
            b'-' => {
                tokens.push(Spanned {
                    token: Token::Minus,
                    at,
                });
                index += 1;
            }
            b'*' => {
                tokens.push(Spanned {
                    token: Token::Star,
                    at,
                });
                index += 1;
            }
            b'/' => {
                tokens.push(Spanned {
                    token: Token::Slash,
                    at,
                });
                index += 1;
            }
            b'(' => {
                tokens.push(Spanned {
                    token: Token::Open,
                    at,
                });
                index += 1;
            }
            b')' => {
                tokens.push(Spanned {
                    token: Token::Close,
                    at,
                });
                index += 1;
            }
            b',' => {
                tokens.push(Spanned {
                    token: Token::Comma,
                    at,
                });
                index += 1;
            }
            b'0'..=b'9' | b'.' => {
                let start = index;
                while index < bytes.len() && (bytes[index].is_ascii_digit() || bytes[index] == b'.')
                {
                    index += 1;
                }
                let text = &source[start..index];
                let value = text.parse::<f64>().map_err(|_| ExprError {
                    problem: format!("`{text}` is not a number"),
                    at: start,
                })?;
                tokens.push(Spanned {
                    token: Token::Number(value),
                    at: start,
                });
            }
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                let start = index;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                tokens.push(Spanned {
                    token: Token::Name(source[start..index].to_ascii_lowercase()),
                    at: start,
                });
            }
            other => {
                return Err(ExprError {
                    problem: format!("`{}` is not part of the language", other as char),
                    at,
                });
            }
        }
    }
    Ok(tokens)
}

struct Parser<'a> {
    tokens: &'a [Spanned],
    position: usize,
    depth: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.position).map(|spanned| &spanned.token)
    }

    fn at(&self) -> usize {
        self.tokens
            .get(self.position)
            .map(|spanned| spanned.at)
            .unwrap_or_else(|| self.tokens.last().map_or(0, |last| last.at))
    }

    fn deeper(&mut self) -> Result<(), ExprError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(ExprError {
                problem: format!("expression nests deeper than {MAX_DEPTH} levels"),
                at: self.at(),
            });
        }
        Ok(())
    }

    /// Precedence climbing. `+ -` bind at 1, `* /` at 2.
    fn expression(&mut self, min_binding: u8) -> Result<Expr, ExprError> {
        self.deeper()?;
        let mut left = self.unary()?;

        while let Some(token) = self.peek() {
            let (op, binding) = match token {
                Token::Plus => (BinOp::Add, 1),
                Token::Minus => (BinOp::Sub, 1),
                Token::Star => (BinOp::Mul, 2),
                Token::Slash => (BinOp::Div, 2),
                _ => break,
            };
            if binding < min_binding {
                break;
            }
            self.position += 1;
            let right = self.expression(binding + 1)?;
            left = Expr::Bin(op, Box::new(left), Box::new(right));
        }

        self.depth -= 1;
        Ok(left)
    }

    fn unary(&mut self) -> Result<Expr, ExprError> {
        if matches!(self.peek(), Some(Token::Minus)) {
            self.position += 1;
            self.deeper()?;
            let inner = self.unary()?;
            self.depth -= 1;
            return Ok(Expr::Neg(Box::new(inner)));
        }
        self.primary()
    }

    fn primary(&mut self) -> Result<Expr, ExprError> {
        let at = self.at();
        let Some(token) = self.peek().cloned() else {
            return Err(ExprError {
                problem: "expression ended early".to_string(),
                at,
            });
        };

        match token {
            Token::Number(value) => {
                self.position += 1;
                Ok(Expr::Const(value))
            }
            Token::Open => {
                self.position += 1;
                self.deeper()?;
                let inner = self.expression(0)?;
                self.depth -= 1;
                self.expect(Token::Close, "an unclosed `(`")?;
                Ok(inner)
            }
            Token::Name(name) => {
                self.position += 1;
                match name.as_str() {
                    "pi" => return Ok(Expr::Const(std::f64::consts::PI)),
                    "tau" => return Ok(Expr::Const(std::f64::consts::TAU)),
                    "s" => return Ok(Expr::Input(Input::S)),
                    "t" => return Ok(Expr::Input(Input::T)),
                    "len" => return Ok(Expr::Input(Input::Len)),
                    "time" => return Ok(Expr::Input(Input::Time)),
                    "boost" => return Ok(Expr::Input(Input::Boost)),
                    "seed" => return Ok(Expr::Input(Input::Seed)),
                    _ => {}
                }

                let Some(func) = Func::parse(&name) else {
                    return Err(ExprError {
                        problem: format!("`{name}` is not a known value or function"),
                        at,
                    });
                };
                self.expect(
                    Token::Open,
                    &format!("`{}` needs its arguments", func.name()),
                )?;

                let mut args = Vec::new();
                if !matches!(self.peek(), Some(Token::Close)) {
                    loop {
                        self.deeper()?;
                        let arg = self.expression(0)?;
                        self.depth -= 1;
                        args.push(arg);
                        if matches!(self.peek(), Some(Token::Comma)) {
                            self.position += 1;
                            continue;
                        }
                        break;
                    }
                }
                self.expect(Token::Close, "an unclosed argument list")?;

                if !func.arity().contains(&args.len()) {
                    return Err(ExprError {
                        problem: format!(
                            "`{}` takes {} argument(s), not {}",
                            func.name(),
                            func.arity()
                                .iter()
                                .map(usize::to_string)
                                .collect::<Vec<_>>()
                                .join(" or "),
                            args.len()
                        ),
                        at,
                    });
                }
                Ok(Expr::Call(func, args))
            }
            other => Err(ExprError {
                problem: format!("{other:?} cannot start an expression"),
                at,
            }),
        }
    }

    fn expect(&mut self, token: Token, problem: &str) -> Result<(), ExprError> {
        if self.peek() == Some(&token) {
            self.position += 1;
            return Ok(());
        }
        Err(ExprError {
            problem: problem.to_string(),
            at: self.at(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eval(source: &str, env: &Env) -> f64 {
        Expr::parse(source)
            .unwrap_or_else(|error| panic!("`{source}` did not parse: {error}"))
            .eval(env)
    }

    /// The example in `specs/skin-shading-prd.md` section 6 has to be
    /// grammatical. An earlier draft of the function list omitted literals,
    /// arithmetic and `tau`, which made the PRD's own example illegal.
    #[test]
    fn the_prd_example_parses_and_evaluates() {
        let expr = Expr::parse("0.3 + 0.1 * sin(tau * time)").expect("grammatical");
        assert_eq!(expr.tier(), Tier::PerStep);

        let at = |time: f64| {
            expr.eval(&Env {
                time,
                ..Env::default()
            })
        };
        assert!((at(0.0) - 0.3).abs() < 1e-12);
        assert!((at(0.25) - 0.4).abs() < 1e-12);
        assert!((at(0.75) - 0.2).abs() < 1e-12);
    }

    #[test]
    fn arithmetic_binds_the_way_arithmetic_does() {
        let env = Env::default();
        assert_eq!(eval("1 + 2 * 3", &env), 7.0);
        assert_eq!(eval("(1 + 2) * 3", &env), 9.0);
        assert_eq!(eval("-2 + 1", &env), -1.0);
        assert_eq!(eval("8 / 4 / 2", &env), 1.0, "division is left-associative");
        assert_eq!(eval("2 - 3 - 4", &env), -5.0, "so is subtraction");
    }

    /// A NaN in an opacity paints nothing at all, which is far harder to debug
    /// than a wrong colour. Every path out of `eval` is finite.
    #[test]
    fn nothing_evaluates_to_a_non_finite_number() {
        let env = Env::default();
        assert_eq!(eval("1 / 0", &env), 0.0);
        assert_eq!(eval("0 / 0", &env), 0.0);
        assert_eq!(eval("1 / (2 - 2)", &env), 0.0);
        for source in ["1.0 / 0", "floor(1 / 0)", "abs(0 / 0)", "sin(1 / 0)"] {
            assert!(
                Expr::parse(source).unwrap().eval(&env).is_finite(),
                "`{source}` escaped as a non-finite value"
            );
        }
    }

    /// The tier is derived from what an expression reads, so an author cannot
    /// claim a per-texel field is constant and find out from the frame rate.
    #[test]
    fn tiers_are_derived_from_what_an_expression_reads() {
        let tier = |source: &str| Expr::parse(source).expect("grammatical").tier();
        assert_eq!(tier("0.5"), Tier::Constant);
        assert_eq!(tier("len * 2 + boost"), Tier::Constant);
        assert_eq!(tier("sin(tau * time)"), Tier::PerStep);
        assert_eq!(tier("s / len"), Tier::PerCell);
        assert_eq!(tier("t + 0.5"), Tier::PerTexel);
        assert_eq!(tier("s + time"), Tier::PerCell, "the more expensive wins");
        assert_eq!(
            tier("noise(1, 2)"),
            Tier::PerTexel,
            "noise varies faster than its arguments suggest"
        );
    }

    /// The tier and the input set answer different questions, and a compiler
    /// that conflated them would fold a boost-reactive expression into a
    /// constant. `boost` and `seed` are constant-*tier* and still per-snake.
    #[test]
    fn inputs_are_reported_separately_from_the_tier() {
        let inputs = |source: &str| Expr::parse(source).expect("grammatical").inputs();

        assert!(inputs("0.3 + 0.2").is_empty(), "a literal reads nothing");
        assert!(inputs("mix(0.7, 1.0, boost)").contains(Input::Boost));
        assert_eq!(
            Expr::parse("mix(0.7, 1.0, boost)").unwrap().tier(),
            Tier::Constant,
            "constant-tier, but emphatically not a constant"
        );

        let both = inputs("sin(tau * time) * len");
        assert!(both.contains(Input::Time) && both.contains(Input::Len));
        assert_eq!(both.first_outside(&[Input::Time]), Some(Input::Len));
        assert_eq!(both.first_outside(&[Input::Time, Input::Len]), None);
        assert_eq!(
            inputs("s / len").iter().collect::<Vec<_>>(),
            vec![Input::S, Input::Len],
            "reported in a stable order, so error messages are stable too"
        );
    }

    #[test]
    fn waveforms_have_the_shapes_they_are_named_after() {
        let env = Env::default();
        assert_eq!(eval("saw(0.25)", &env), 0.25);
        assert_eq!(eval("saw(1.25)", &env), 0.25);
        assert_eq!(
            eval("saw(-0.75)", &env),
            0.25,
            "saw wraps for negatives too"
        );
        assert_eq!(eval("tri(0.0)", &env), 0.0);
        assert_eq!(eval("tri(0.5)", &env), 1.0);
        assert_eq!(eval("tri(1.0)", &env), 0.0);
        assert_eq!(eval("pulse(0.2)", &env), 1.0);
        assert_eq!(eval("pulse(0.7)", &env), 0.0);
        assert_eq!(eval("pulse(0.7, 0.8)", &env), 1.0, "duty is adjustable");
        assert_eq!(eval("step(0.5, 0.6)", &env), 1.0);
        assert_eq!(eval("step(0.5, 0.4)", &env), 0.0);
        assert_eq!(eval("mix(10, 20, 0.25)", &env), 12.5);
        assert_eq!(eval("clamp(5, 0, 1)", &env), 1.0);
        assert_eq!(
            eval("clamp(5, 1, 0)", &env),
            1.0,
            "a reversed range still clamps"
        );
        assert_eq!(eval("smoothstep(0, 1, 0.5)", &env), 0.5);
        assert_eq!(eval("smoothstep(1, 1, 2)", &env), 1.0, "a zero-width edge");
    }

    /// Noise has to be identical everywhere: two players in one match must see
    /// the same snake, and a platform-dependent field would be a cosmetic
    /// desync nobody would think to look for.
    #[test]
    fn noise_is_deterministic_bounded_and_not_constant() {
        let sample = |x: f64, y: f64| value_noise(x, y);
        assert_eq!(sample(1.5, 2.5), sample(1.5, 2.5));
        assert_ne!(sample(1.5, 2.5), sample(2.5, 1.5));

        let mut seen_low = false;
        let mut seen_high = false;
        for i in 0..200 {
            let value = sample(i as f64 * 0.37, i as f64 * 0.11);
            assert!(
                (0.0..=1.0).contains(&value),
                "noise left its range: {value}"
            );
            seen_low |= value < 0.3;
            seen_high |= value > 0.7;
        }
        assert!(seen_low && seen_high, "noise is not varying");
    }

    /// The sandbox boundary. These are errors, not warnings, and they are the
    /// property that must survive any future relaxation for ergonomics.
    #[test]
    fn the_language_rejects_everything_outside_it() {
        for source in [
            "while(1)",
            "f(x) = x",
            "s[0]",
            "1 ** 2",
            "sin",
            "sin(1, 2)",
            "clamp(1, 2)",
            "(1 + 2",
            "1 +",
            "",
            "nonesuch(1)",
            "1 2",
            // Exponent notation is not in the grammar: `decimal numbers` means
            // decimal numbers, and `1e0` reads as a number beside a name.
            "1e0",
        ] {
            assert!(
                Expr::parse(source).is_err(),
                "`{source}` was accepted, which widens the sandbox"
            );
        }
    }

    /// A parser without a depth cap turns a long string of `(` into a stack
    /// overflow, which is a crash rather than an error message.
    #[test]
    fn deep_nesting_is_an_error_rather_than_a_crash() {
        let deep = format!("{}1{}", "(".repeat(400), ")".repeat(400));
        let error = Expr::parse(&deep).expect_err("400 levels is past the cap");
        assert!(error.problem.contains("nests deeper"));

        // ...and something reasonable still parses.
        assert!(Expr::parse("((((1 + 2))))").is_ok());
    }

    #[test]
    fn errors_point_at_the_offending_byte() {
        let error = Expr::parse("1 + nonesuch(2)").expect_err("unknown name");
        assert_eq!(error.at, 4);
        assert!(error.problem.contains("nonesuch"));
    }
}
