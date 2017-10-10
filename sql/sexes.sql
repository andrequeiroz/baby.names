CREATE TABLE sexes (
  sex_code CHAR(1) PRIMARY KEY,
  sex_label VARCHAR(6) UNIQUE NOT NULL
);

INSERT INTO sexes VALUES
('F', 'Female'),
('M', 'Male');
