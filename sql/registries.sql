CREATE TABLE registries (
  nm_code INTEGER NOT NULL,
  sex_code CHAR(1) NOT NULL,
  yob SMALLINT NOT NULL,
  st_abbr CHAR(2),
  total INTEGER NOT NULL,
  FOREIGN KEY (nm_code) REFERENCES names (nm_code),
  FOREIGN KEY (sex_code) REFERENCES sexes (sex_code),
  FOREIGN KEY (st_abbr) REFERENCES states (st_abbr)
);
