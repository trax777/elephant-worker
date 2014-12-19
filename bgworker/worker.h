/* ------------------------------------------------------------------------
 * worker.h
 *  	Function exports from the worker process.
 *
 * Copyright (c) 2014, Zalando SE.
 * Portions Copyright (C) 2013-2014, PostgreSQL Global Development Group
 * ------------------------------------------------------------------------
 */

 #ifndef _WORKER_H
 #define _WORKER_H

 #include 	"postgres.h"

 void		worker__main(Datum) __attribute__((noreturn));
 #endif