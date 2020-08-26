# clj-filesystem

An unified sdk for several object storage system.

## Releases and Dependency Information

This project follows the version scheme MAJOR.MINOR.PATCH where each component provides some relative indication of the size of the change, but does not follow semantic versioning. In general, all changes endeavor to be non-breaking (by moving to new names rather than by breaking existing names).

Latest stable release is [0.1.0]

[Leiningen](http://leiningen.org/) dependency information:

```clojure
[clj-filesystem "1.0.0"]
```

## Usage

Example usage

```clojure
(ns example
  (:require [clj-filesystem.core :as clj-fs]))
```

To setup connection for an object storage system

```clojure
(def fs-service "oss")
(def fs-endpoint "http://oss-cn-shanghai.aliyuncs.com")
(def fs-access-key "")
(def fs-secret-key "")
(clj-fs/setup-connection fs-service fs-endpoint fs-access-key fs-secret-key)
```

To list buckets

```clojure
(clj-fs/list-buckets)
```

To list objects

```clojure
(clj-fs/list-objects "bucket-name")
```

## License

Copyright © 2020 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
