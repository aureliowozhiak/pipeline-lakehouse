package br.lakehouse.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Sistema de arquivos local para Hadoop/Spark no Windows sem winutils.exe.
 *
 * O RawLocalFileSystem padrao chama winutils.exe para aplicar/ler permissoes
 * POSIX (chmod/chown/ls). No Windows essas permissoes nao tem efeito pratico
 * para um lakehouse local, entao esta classe as ignora.
 *
 * Uso: spark.hadoop.fs.file.impl=br.lakehouse.hadoop.BareLocalFileSystem
 */
public class BareLocalFileSystem extends LocalFileSystem {

    public BareLocalFileSystem() {
        super(new BareRawLocalFileSystem());
    }

    public static class BareRawLocalFileSystem extends RawLocalFileSystem {

        @Override
        public void setPermission(Path p, FsPermission permission) throws IOException {
            // sem efeito no Windows
        }

        @Override
        public void setOwner(Path p, String username, String groupname) throws IOException {
            // sem efeito no Windows
        }

        @Override
        public FileStatus getFileStatus(Path f) throws IOException {
            return toBareStatus(super.getFileStatus(f));
        }

        @Override
        public FileStatus getFileLinkStatus(Path f) throws IOException {
            return toBareStatus(super.getFileLinkStatus(f));
        }

        /**
         * Reimplementado com java.io.File: a versao original usa
         * FileUtil.list -> NativeIO (hadoop.dll), ausente sem winutils.
         */
        @Override
        public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
            File localf = pathToFile(f);
            if (!localf.exists()) {
                throw new FileNotFoundException("File " + f + " does not exist");
            }
            if (!localf.isDirectory()) {
                return new FileStatus[] {getFileStatus(f)};
            }
            String[] names = localf.list();
            if (names == null) {
                throw new IOException("Cannot list directory " + f);
            }
            List<FileStatus> result = new ArrayList<FileStatus>(names.length);
            for (String name : names) {
                try {
                    result.add(getFileStatus(new Path(f, new Path(null, null, name))));
                } catch (FileNotFoundException e) {
                    // arquivo removido durante a listagem
                }
            }
            return result.toArray(new FileStatus[0]);
        }

        private static FileStatus toBareStatus(FileStatus s) {
            if (s == null) {
                return null;
            }
            FsPermission perm = s.isDirectory()
                    ? FsPermission.getDirDefault()
                    : FsPermission.getFileDefault();
            return new FileStatus(
                    s.getLen(), s.isDirectory(), s.getReplication(), s.getBlockSize(),
                    s.getModificationTime(), s.getAccessTime(), perm,
                    "user", "group", s.getPath());
        }
    }
}
