-- MySQL dump 10.13  Distrib 8.0.20, for Win64 (x86_64)
--
-- Host: 127.0.0.1    Database: datawarehouse
-- ------------------------------------------------------
-- Server version	8.0.20

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `student`
--

DROP TABLE IF EXISTS `student`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `student` (
  `stt` int NOT NULL AUTO_INCREMENT,
  `id_student` int NOT NULL,
  `first_name` varchar(100) DEFAULT NULL,
  `last_name` varchar(100) DEFAULT NULL,
  `date_of_birth` date DEFAULT NULL,
  `class_id` varchar(100) DEFAULT NULL,
  `class_name` varchar(100) DEFAULT NULL,
  `phone` varchar(100) DEFAULT NULL,
  `email` varchar(100) DEFAULT NULL,
  `address` varchar(100) DEFAULT NULL,
  `note` varchar(100) DEFAULT NULL,
  `date_expire` date DEFAULT NULL,
  PRIMARY KEY (`stt`)
) ENGINE=InnoDB AUTO_INCREMENT=22 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `student`
--

LOCK TABLES `student` WRITE;
/*!40000 ALTER TABLE `student` DISABLE KEYS */;
INSERT INTO `student` VALUES (1,17130071,'Thái Nguyễn Ngân','Anh','1999-06-05','DH17KM','Kinh tế tài nguyên môi trường','343288777','17130071@st.hcmuaf.edu.vn','Chánh An, Cát Hanh, Phù Cát, Bình Định.','Hộ nghèo','2020-06-29'),(2,17123005,'Nguyễn Thị Huệ','Châu','1999-07-08','DH17KM','Kinh tế tài nguyên môi trường','365826233','17123005@st.hcmuaf.edu.vn','Số 396 đường Tây Sơn, Phường Quang Trung, Thành phố Quy Nhơn, Tỉnh Bình Định.','Hộ nghèo','9999-12-31'),(3,17120014,'Dương Phương','Di','1999-12-20','DH17KM','Kinh tế tài nguyên môi trường','345672873','17120014@st.hcmuaf.edu.vn','Số 985 đường Trần Hưng Đạo, Phường Đống Đa, Thành phố Quy Nhơn, Tỉnh Bình Định.','Hộ nghèo','9999-12-31'),(4,17120021,'Trần Thị','Diễm','1999-08-01','DH17KM','Kinh tế tài nguyên môi trường','345672873','17120021@st.hcmuaf.edu.vn','Thôn Tân Phú, Xã Mỹ Đức, Huyện Phù Mỹ, Tỉnh Bình Định.','Hộ nghèo','9999-12-31'),(5,17120022,'Nguyễn Thị','Diệu','1999-09-09','DH17KM','Kinh tế tài nguyên môi trường','345672873','17120022@st.hcmuaf.edu.vn','Số 91 đường Lê Duẩn, Phường Đập Đá, Thị xã An Nhơn, Tỉnh Bình Định.','Hộ nghèo','9999-12-31'),(6,17120024,'Bùi Thị','Dung','1999-01-03','DH17KM','Kinh tế tài nguyên môi trường','345672873','17120024@st.hcmuaf.edu.vn','Số 25/3/2 đường Lý Thái Tổ, Phường Nguyễn Văn Cừ, Thành phố Quy Nhơn, Tỉnh Bình Định.','x','9999-12-31'),(7,17123010,'Lê Thị Mỹ','Duyên','1999-11-27','DH17KM','Kinh tế tài nguyên môi trường','345672873','17123010@st.hcmuaf.edu.vn','Số 351 đường Trần Phú, Thị Trấn Diêu Trì, Huyện Tuy Phước, Tỉnh Bình Định.','xx','9999-12-31'),(8,17123016,'Trần Thảo','Duyên','1999-12-10','DH17KM','Kinh tế tài nguyên môi trường','345672873','17123016@st.hcmuaf.edu.vn','Lô 18B đường Hoa Lư, Phường Nhơn Bình, Thành phố Quy Nhơn, Tỉnh Bình Định.','Hộ nghèo','9999-12-31'),(9,17120033,'Nguyễn Thị Kim','Giang','1999-11-06','DH17KM','Kinh tế tài nguyên môi trường','345672873','17120033@st.hcmuaf.edu.vn','Số 718/12 đường Trần Hưng Đạo, Phường Đống Đa, Thành phố Quy Nhơn, Tỉnh Bình Định.','x','9999-12-31'),(10,16123053,'Nguyễn Minh','Hảo','1999-03-22','DH17KM','Kinh tế tài nguyên môi trường','345672873','16123053@st.hcmuaf.edu.vn','Số 09 đường Trần Can, Phường Quang Trung, Thành phố Quy Nhơn, Tỉnh Bình Định.','x','9999-12-31'),(11,17120042,'Nguyễn Thị','Hằng','1999-02-02','DH17KM','Kinh tế tài nguyên môi trường','345672873','17120042@st.hcmuaf.edu.vn','Phường Bùi Thị Xuân, Thành phố Quy Nhơn, Tỉnh Bình Định.','Hộ nghèo','9999-12-31'),(12,17120020,'Lê Phúc','Hậu','1999-01-04','DH17KM','Kinh tế tài nguyên môi trường','345672873','17120020@st.hcmuaf.edu.vn','Số 12 đường Nguyễn Thi, Thành phố Quy Nhơn, Tỉnh Bình Định.','x','9999-12-31'),(13,17120052,'Nguyễn Thị','Hiền','1999-05-09','DH17KM','Kinh tế tài nguyên môi trường','345672873','17120052@st.hcmuaf.edu.vn','Chánh An, Cát Hanh, Phù Cát, Bình Định.','x','9999-12-31'),(14,18120076,'Lê Minh','Hồ','1999-05-06','DH17KM','Kinh tế tài nguyên môi trường','345672873','18120076@st.hcmuaf.edu.vn','Số 21 đường Huỳnh Thị Đào, Phường Nhơn Bình, Thành phố Quy Nhơn, Tỉnh Bình Định.','x','9999-12-31'),(15,17120057,'Đặng Thị','Hương','1999-08-03','DH17KM','Kinh tế tài nguyên môi trường','345672873','17120057@st.hcmuaf.edu.vn','Thôn Liễu An Nam, Xã Hoài Châu Bắc, Thị xã Hoài Nhơn, Tỉnh Bình Định.','x','9999-12-31'),(16,17120058,'Nguyễn Lê','Kha','1999-01-02','DH17KM','Kinh tế tài nguyên môi trường','345672873','17120058@st.hcmuaf.edu.vn','Số 15 đường Xuân Thủy, Phường Quang Trung, Thành phố Quy Nhơn, Tỉnh Bình Định.','x','9999-12-31'),(17,17120060,'Nguyễn Thị','Lưu','1999-03-01','DH17KM','Kinh tế tài nguyên môi trường','345672873','17120060@st.hcmuaf.edu.vn','Số 57 đường Nguyễn Hữu Tiến, Thành phố Quy Nhơn, Tỉnh Bình Định.','x','9999-12-31'),(18,17120068,'Nguyễn Châu','Khoa','1999-06-02','DH17KM','Kinh tế tài nguyên môi trường','345672873','17120068@st.hcmuaf.edu.vn','Số 153/5 đường Chương Dương, Thành phố Quy Nhơn, Tỉnh Bình Định.','x','9999-12-31'),(19,17120086,'Phan Đình','Khôi','1999-01-04','DH17KM','Kinh tế tài nguyên môi trường','345672873','17120086@st.hcmuaf.edu.vn','Khu phố Công Thạnh, Phường Tam Quan Bắc, Thị xã Hoài Nhơn, Tỉnh Bình Định.','x','9999-12-31'),(20,17120069,'Trần thị','Mỹ','1999-02-04','DH17KM','Kinh tế tài nguyên môi trường','345672873','17120069@st.hcmuaf.edu.vn','Phường Quang Trung, Thành phố Quy Nhơn, Bình Định.','Hộ nghèo','9999-12-31'),(21,17130071,'Thái Nguyễn Ngân','Anh','1999-06-05','DH17DTA','Kinh tế tài nguyên môi trường','343288777','17130071@st.hcmuaf.edu.vn','Chánh An, Cát Hanh, Phù Cát, Bình Định.','Hộ nghèo','9999-12-31');
/*!40000 ALTER TABLE `student` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2020-06-29  2:01:50
