CREATE TABLE `sessions` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `user_id` INT,
  `session_token` VARCHAR(255),
  `ip_address` VARCHAR(45),
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE `users` (
  `id` INT PRIMARY KEY,
  `user_name` VARCHAR(255),
  `email` VARCHAR(255),
  `last_login` TIMESTAMP
);

CREATE TABLE `products` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `product_name` VARCHAR(255),
  `price` DECIMAL(10, 2),
  `category` VARCHAR(100),
  `product_sku` VARCHAR(100)
);

CREATE TABLE `orders` (
  `id` INT PRIMARY KEY,
  `status` VARCHAR(50),
  `updated_at` TIMESTAMP
);

CREATE TABLE `logs` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `level` VARCHAR(50),
  `event_id` VARCHAR(255),
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
