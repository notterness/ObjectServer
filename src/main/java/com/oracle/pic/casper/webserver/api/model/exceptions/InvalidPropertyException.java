package com.oracle.pic.casper.webserver.api.model.exceptions;


public class InvalidPropertyException extends RuntimeException {
   public InvalidPropertyException(String msg, Throwable cause) {
       super(msg, cause);
   }
}
