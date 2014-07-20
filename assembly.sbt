import AssemblyKeys._ // put this at the top of the file

assemblySettings


mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("javax", "xml", xs @ _*) => MergeStrategy.first
    case PathList("META-INF", "maven", "joda-time", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "jasper", xs @ _*) => MergeStrategy.first
    case PathList("org", "joda", "time", xs @ _*) => MergeStrategy.first
    case PathList("javax", "servlet", "jsp", xs @ _*) => MergeStrategy.first
    case x => old(x)
  }
}

