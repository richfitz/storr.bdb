##' Berkeley DB storr driver.
##'
##' @section Design considerations:
##'
##' Because there is no easy way of iterating over all keys (without
##' retrieving all data), the data is stored in two different
##' databases within the file; one called \code{data} the other called
##' \code{keys}.  This should make \code{list_keys} a bit faster, but
##' \code{list_data} will always be very slow and memory intensive.
##' @title Berkeley DB storr driver
##' @param path Path for the database.  If \code{NULL} a transient
##'   in-memory database will be created.
##' @param type Storage type for all the data.  Sensible options are
##'   \code{BTREE} and \code{HASH}.
##' @param default_namespace Default namespace (see
##'   \code{\link{storr}}).
##' @export
##' @examples
##' st <- storr_bdb(NULL)
storr_bdb <- function(path, type="BTREE", default_namespace="objects") {
  storr::storr(driver_bdb(path, type), default_namespace)
}

##' @export
##' @rdname storr_bdb
driver_bdb <- function(path, type="BTREE") {
  .R6_driver_bdb$new(path, type)
}

##' @importFrom R6 R6Class
.R6_driver_bdb <- R6::R6Class(
  "driver_bdb",
  public=list(
    keys=NULL,
    data=NULL,
    path=NULL,

    initialize=function(path, type) {
      flags <- RBerkeley::mkFlags("DB_CREATE")
      self$path <- path
      self$keys <- bdb_create(database="keys", file=path, type=type,
                              flags=flags)
      self$data <- bdb_create(database="data", file=path, type=type,
                              flags=flags)
    },

    type=function() {
      "bdb"
    },
    destroy=function() {
      ## Need to actually destroy the db here; can we get the
      self$keys <- bdb_destroy(self$keys, "keys")
      self$data <- bdb_destroy(self$data, "data")
    },

    get_hash=function(key, namespace) {
      rawToChar(RBerkeley::db_get(self$keys, key=self$name_key(key, namespace)))
    },
    set_hash=function(key, namespace, hash) {
      RBerkeley::db_put(self$keys, key=self$name_key(key, namespace),
                        data=charToRaw(hash))
    },
    get_object=function(hash) {
      bin_to_object(RBerkeley::db_get(self$data, key=self$name_hash(hash)))
    },
    set_object=function(hash, value) {
      RBerkeley::db_put(self$data, key=self$name_hash(hash),
                        data=object_to_bin(value))
    },

    exists_key=function(key, namespace) {
      RBerkeley::db_exists(self$keys, key=self$name_key(key, namespace)) == 0L
    },
    exists_hash=function(hash) {
      RBerkeley::db_exists(self$data, key=self$name_hash(hash)) == 0L
    },

    del_key=function(key, namespace) {
      RBerkeley::db_del(self$keys, key=self$name_key(key, namespace)) == 0L
    },
    del_hash=function(hash) {
      RBerkeley::db_del(self$data, key=self$name_hash(hash)) == 0L
    },

    list_keys=function(namespace) {
      re <- sprintf("^%s:", namespace)
      sub(re, "", grep(re, bdb_scan_keys(self$keys), value=TRUE))
    },
    list_namespaces=function() {
      unique(sub(":.+$", "", bdb_scan_keys(self$keys)))
    },
    list_hashes=function() {
      bdb_scan_keys(self$data)
    },

    ## Not 100% sure if this is the best bet here.  Could use
    ## something like __namespace__::key?  Basically avoiding
    ## colisions is hard.  In Redis we use namespace:key I think.
    name_hash=function(hash) {
      charToRaw(hash)
    },
    name_key=function(key, namespace) {
      charToRaw(paste0(namespace, ":", key))
    }
  ))

bdb_create <- function(...) {
  dbh <- RBerkeley::db_create()
  res <- RBerkeley::db_open(dbh, ...)
  if (res != 0L) {
    stop(RBerkeley::db_strerror(res))
  }
  dbh
}
bdb_destroy <- function(dbh, file, database) {
  ok <- suppressWarnings(RBerkeley::db_close(dbh))
  ## RBerkeley::db_remove(dbh, file, database)
  NULL
}
bdb_scan_keys <- function(dbh, n=100) {
  flags <- RBerkeley::mkFlags("DB_NEXT")
  dbc <- RBerkeley::db_cursor(dbh)
  ret <- character()
  repeat {
    res <- RBerkeley::dbcursor_get(dbc, flags=flags, n=n)
    if (!is.list(res)) {
      break
    }
    res <- vapply(res, function(x) rawToChar(x$key), character(1))
    ret <- c(ret, res)
  }
  ret
}

object_to_bin <- function(x) {
  serialize(x, NULL)
}

bin_to_object <- function(x) {
  unserialize(x)
}
