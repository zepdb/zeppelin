/** Base error class for Zeppelin client errors. */
export class ZeppelinError extends Error {
  public readonly statusCode: number | undefined;

  constructor(message: string, statusCode?: number) {
    super(message);
    this.name = "ZeppelinError";
    this.statusCode = statusCode;
  }
}

/** Invalid request (HTTP 400). */
export class ValidationError extends ZeppelinError {
  constructor(message: string) {
    super(message, 400);
    this.name = "ValidationError";
  }
}

/** Resource not found (HTTP 404). */
export class NotFoundError extends ZeppelinError {
  constructor(message: string) {
    super(message, 404);
    this.name = "NotFoundError";
  }
}

/** Resource conflict (HTTP 409). */
export class ConflictError extends ZeppelinError {
  constructor(message: string) {
    super(message, 409);
    this.name = "ConflictError";
  }
}

/** Server-side error (HTTP 5xx). */
export class ServerError extends ZeppelinError {
  constructor(message: string, statusCode: number = 500) {
    super(message, statusCode);
    this.name = "ServerError";
  }
}
