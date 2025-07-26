//! Defines a Domain-Specific Language (DSL) for building type-safe AQL queries.
//!
//! This module provides the `Expression` enum and related structures to represent
//! query components like fields, literals, and operations, allowing for the
//! programmatic construction of complex, safe AQL filters.

use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;

/// Represents a part of an AQL query, forming an expression tree.
#[derive(Clone, Debug)]
pub enum Expression {
    /// A literal value, like a number or string.
    Literal(Value),
    /// A reference to a component's field, e.g., `doc.Health.value`.
    Field {
        component_name: &'static str,
        field_name: &'static str,
    },
    /// A binary operation, like `==`, `>`, `AND`.
    BinaryOp {
        op: BinaryOperator,
        lhs: Box<Expression>,
        rhs: Box<Expression>,
    },
}

/// Represents the different binary operators available in AQL.
#[derive(Clone, Debug)]
pub enum BinaryOperator {
    Eq, Gt, Gte, Lt, Lte, And, Or,
}

impl Expression {
    /// Combines this expression with another using the `AND` operator.
    pub fn and(self, other: Expression) -> Expression {
        Expression::BinaryOp {
            op: BinaryOperator::And,
            lhs: Box::new(self),
            rhs: Box::new(other),
        }
    }

    /// Combines this expression with another using the `OR` operator.
    pub fn or(self, other: Expression) -> Expression {
        Expression::BinaryOp {
            op: BinaryOperator::Or,
            lhs: Box::new(self),
            rhs: Box::new(other),
        }
    }

    /// Creates an equality comparison (`==`).
    pub fn eq<T: Serialize>(self, value: T) -> Expression {
        Expression::BinaryOp {
            op: BinaryOperator::Eq,
            lhs: Box::new(self),
            rhs: Box::new(Expression::Literal(serde_json::to_value(value).unwrap())),
        }
    }

    /// Creates a greater-than comparison (`>`).
    pub fn gt<T: Serialize>(self, value: T) -> Expression {
        Expression::BinaryOp {
            op: BinaryOperator::Gt,
            lhs: Box::new(self),
            rhs: Box::new(Expression::Literal(serde_json::to_value(value).unwrap())),
        }
    }

    /// Creates a greater-than-or-equal comparison (`>=`).
    pub fn gte<T: Serialize>(self, value: T) -> Expression {
        Expression::BinaryOp {
            op: BinaryOperator::Gte,
            lhs: Box::new(self),
            rhs: Box::new(Expression::Literal(serde_json::to_value(value).unwrap())),
        }
    }

    /// Creates a less-than comparison (`<`).
    pub fn lt<T: Serialize>(self, value: T) -> Expression {
        Expression::BinaryOp {
            op: BinaryOperator::Lt,
            lhs: Box::new(self),
            rhs: Box::new(Expression::Literal(serde_json::to_value(value).unwrap())),
        }
    }

    /// Creates a less-than-or-equal comparison (`<=`).
    pub fn lte<T: Serialize>(self, value: T) -> Expression {
        Expression::BinaryOp {
            op: BinaryOperator::Lte,
            lhs: Box::new(self),
            rhs: Box::new(Expression::Literal(serde_json::to_value(value).unwrap())),
        }
    }
}

/// Translates an `Expression` tree into an AQL string and bind variables.
pub(crate) fn translate_expression(
    expr: &Expression,
    bind_vars: &mut HashMap<String, Value>,
) -> String {
    match expr {
        Expression::Literal(val) => {
            let bind_name = format!("bevy_arangodb_bind_{}", bind_vars.len());
            bind_vars.insert(bind_name.clone(), val.clone());
            format!("@{}", bind_name)
        }
        Expression::Field { component_name, field_name } => {
            format!("doc.`{}`.`{}`", component_name, field_name)
        }
        Expression::BinaryOp { op, lhs, rhs } => {
            let op_str = match op {
                BinaryOperator::Eq => "==",
                BinaryOperator::Gt => ">",
                BinaryOperator::Gte => ">=",
                BinaryOperator::Lt => "<",
                BinaryOperator::Lte => "<=",
                BinaryOperator::And => "AND",
                BinaryOperator::Or => "OR",
            };
            let lhs_str = translate_expression(lhs, bind_vars);
            let rhs_str = translate_expression(rhs, bind_vars);
            format!("({} {} {})", lhs_str, op_str, rhs_str)
        }
    }
}
