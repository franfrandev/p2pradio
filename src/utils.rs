use crate::AppError;
use std::convert::Infallible;
use tokio::task::JoinError;

pub trait Ctx {}

pub(crate) trait AsyncService<Ctx, E1: Into<AppError>, E2: Into<AppError>> {
    async fn new(ctx: Ctx) -> Result<Self, E1>
    where
        Self: Sized;

    async fn run(self) -> TaskRet<E2>;
}

pub(crate) trait Service<Ctx, E1: Into<AppError>, E2: Into<AppError>> {
    fn new(ctx: Ctx) -> Result<Self, E1>
    where
        Self: Sized;

    async fn run(self) -> TaskRet<E2>;
}

pub type TaskRet<E> = Option<Result<Infallible, E>>;

pub fn handle_to_main_handle<E: Into<AppError>>(
    res: Result<TaskRet<E>, JoinError>,
) -> TaskRet<AppError> {
    match res {
        Err(err) => Some(Err(AppError::TaskPanicked(err))), // panicked
        Ok(None) => None,                                   // cancelled
        Ok(Some(Err(err))) => Some(Err(err.into())),        // errored
    }
}

pub fn handle_loop_cannot_fail_handle(
    res: Result<TaskRet<Infallible>, JoinError>,
) -> TaskRet<AppError> {
    match res {
        Err(err) => Some(Err(AppError::TaskPanicked(err))), // panicked
        Ok(None) => None,                                   // cancelled
    }
}

pub fn handle_loop_can_exit(res: Result<Option<()>, JoinError>) -> TaskRet<AppError> {
    match res {
        Err(err) => Some(Err(AppError::TaskPanicked(err))), // panicked
        Ok(None) => None,                                   // cancelled
        Ok(Some(())) => Some(Err(AppError::TaskExited)),    // exited
    }
}
