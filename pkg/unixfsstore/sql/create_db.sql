CREATE TABLE IF NOT EXISTS DirLinks (
  RootCID BLOB NOT NULL,
  Metadata BLOB,
  CID BLOB NOT NULL,
  Depth INT NOT NULL,
  Leaf INT NOT NULL,
  SubPath TEXT NOT NULL,
  PRIMARY KEY(RootCID, Metadata, SubPath, Depth)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS FileLinks (
  RootCID BLOB NOT NULL,
  Metadata BLOB,
  CID BLOB NOT NULL,
  Depth INT NOT NULL,
  Leaf INT NOT NULL,
  ByteMin INT NOT NULL,
  ByteMax INT NOT NULL,
  PRIMARY Key(RootCID, Metadata, Depth, ByteMin, ByteMax)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS RootCIDs (
  CID BLOB NOT NULL,
  Kind INT NOT NULL,
  Metadata BLOB,
  PRIMARY KEY(CID, Metadata)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS index_dir_links_root_cid on DirLinks(RootCID, Metadata);
CREATE INDEX IF NOT EXISTS index_dir_links_root_cid_sub_path on DirLinks(RootCID, Metadata, SubPath);
CREATE INDEX IF NOT EXISTS index_file_links_root_cid on FileLinks(RootCID, Metadata);
CREATE INDEX IF NOT EXISTS index_file_links_root_cid_byte_min_max on FileLinks(RootCID, Metadata, ByteMin, ByteMax);
CREATE INDEX IF NOT EXISTS index_root_cids_cid on RootCIDS(CID)