use super::OperationKind;

#[derive(Debug, Clone)]
pub enum FunctionOperation {
    Create {
        schema: String,
        name: String,
        arguments: String,
        #[allow(dead_code)]
        kind: String,
        #[allow(dead_code)]
        parameters: String,
        #[allow(dead_code)]
        returns: String,
        #[allow(dead_code)]
        attributes: String,
        definition: String,
    },
    Replace {
        schema: String,
        name: String,
        arguments: String,
        #[allow(dead_code)]
        kind: String,
        #[allow(dead_code)]
        parameters: String,
        #[allow(dead_code)]
        returns: String,
        #[allow(dead_code)]
        attributes: String,
        definition: String,
    },
    Drop {
        schema: String,
        name: String,
        arguments: String,
        kind: String,
        parameter_types: String,
    },
}

impl FunctionOperation {
    pub fn operation_kind(&self) -> OperationKind {
        match self {
            Self::Create { .. } => OperationKind::Create,
            Self::Drop { .. } => OperationKind::Drop,
            Self::Replace { .. } => OperationKind::Alter,
        }
    }
}
