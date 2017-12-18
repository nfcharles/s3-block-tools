(defproject blocks "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.374"]
		 [org.clojure/data.json "0.2.6"]
                 [com.fasterxml.jackson.core/jackson-core "2.9.3"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.9.3"]
		 [com.amazonaws/aws-java-sdk-core "1.11.242"]
                 [com.amazonaws/aws-java-sdk-s3 "1.11.242"]
		 [com.amazonaws/aws-java-sdk-lambda "1.11.242"]
                 [org.apache.hadoop/hadoop-core "1.2.1"]
		 [org.apache.hadoop/hadoop-aws "2.7.3" :exclusions [joda-time]]]
  :main ^:skip-aot blocks.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})