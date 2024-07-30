USE guitarrasmlscrapeo;
CREATE TABLE IF NOT EXISTS guitarras (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    price VARCHAR(50) NOT NULL,
    href TEXT NOT NULL
);