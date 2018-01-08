(ns blocks.error)

(gen-class
  :name blocks.error.InvalidBlockIndexFormatException
  :extends java.lang.Exception)

(gen-class
  :name blocks.error.UnknownDataSourceException
  :extends java.lang.Exception)
