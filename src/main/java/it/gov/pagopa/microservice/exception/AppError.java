package it.gov.pagopa.microservice.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public enum AppError {
  INTERNAL_SERVER_ERROR(
      HttpStatus.INTERNAL_SERVER_ERROR, "Internal Server Error", "Something was wrong"),
  BAD_REQUEST(HttpStatus.INTERNAL_SERVER_ERROR, "Bad Request", "%s"),
  UNAUTHORIZED(HttpStatus.UNAUTHORIZED, "Unauthorized", "Error during authentication"),
  FORBIDDEN(HttpStatus.FORBIDDEN, "Forbidden", "This method is forbidden"),
  RESPONSE_NOT_READABLE(
      HttpStatus.BAD_GATEWAY, "Response Not Readable", "The response body is not readable"),

  UNKNOWN(null, null, null);

  public final HttpStatus httpStatus;
  public final String title;
  public final String details;

  AppError(HttpStatus httpStatus, String title, String details) {
    this.httpStatus = httpStatus;
    this.title = title;
    this.details = details;
  }
}
