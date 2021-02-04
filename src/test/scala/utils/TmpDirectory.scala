package utils

import com.google.common.io.Files
import org.apache.commons.io.FileExistsException

import java.io.{BufferedWriter, File, FileWriter, IOException}
import scala.io.Source


trait TmpDirectory {

  implicit class RichBoolean(bool: Boolean) {
    def toOption: Option[Boolean] = if (bool) Option(true) else None
  }
  
  def assertDirExists(dirPath: String): Unit = {
    assert(new File(dirPath).isDirectory, s"Directory in path '$dirPath' doesn't exist")
  }

  def assertDirNotExists(dirPath: String): Unit = {
    assert(!new File(dirPath).isDirectory, s"Directory exists in path '$dirPath'")
  }
  
  def assertFileExists(filePath: String): Unit = {
    assert(new File(filePath).isFile, s"File in path '$filePath' doesn't exist")
  }

  def assertFileNotExists(filePath: String): Unit = {
    assert(!new File(filePath).isFile, s"File in path '$filePath' doesn't exist")
  }

  def assertFileContentIs(filePath: String, expectedContent: String): Unit = {
    val source = Source.fromFile(filePath)
    val actualContent = source.mkString("")
    source.close()
    assert(expectedContent == actualContent, s"Actual content is:\n$actualContent BUT EXPECTED \n$expectedContent\nEndOfExpected")
  }

  def getFileContent(filePath: String): String = {
    val source = Source.fromFile(filePath)
    val actualContent = source.mkString("")
    source.close()
    actualContent
  }

  def createFile(dirPath: String, fileName: String, content: String): String = {
    val dir = new File(dirPath)
    if (!dir.exists) dir.mkdirs()
    val file = new File(s"$dir/$fileName")
    if (file.exists) throw new FileExistsException(s"File ${file.getAbsolutePath} already exists")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(content)
    bw.flush()
    bw.close()
    file.getAbsolutePath
  }

  def createDir(dirPath: String): Unit = {
    val dir = new File(dirPath)
    if (!dir.exists) dir.mkdirs()
  }

  def withTempDir(f: File => Unit): Unit = {
    val dir: File = Files.createTempDir()
    dir.mkdir()
    try f(dir) finally deleteRecursively(dir)
  }

  private def deleteRecursively(file: File) {
    if (file != null) {
      try {
        if (file.isDirectory) {
          var savedIOException: IOException = null
          for (child <- listFiles(file)) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
        }
      } finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }

  def listFiles(dir: String): Seq[File] = {
    listFiles(new File(dir))
  }

  def listFiles(dir: File): Seq[File] = {
    dir.exists.toOption.map(_ => Option(dir.listFiles()).map(_.toSeq).getOrElse {
      throw new IOException(s"Failed to list files for dir: $dir")
    }).getOrElse(Seq.empty[File])
  }

}
