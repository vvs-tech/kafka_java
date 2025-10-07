package main.java.sbx;

import java.io.FileInputStream;
import java.security.KeyStore;

public class JKSChecker {

    public static boolean validateJKS(String filePath, String password) {
        try (FileInputStream fis = new FileInputStream(filePath)) {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(fis, password.toCharArray());
            System.out.println("✓ JKS файл валиден: " + filePath);
            System.out.println("  Тип: " + keyStore.getType());
            System.out.println("  Размер: " + keyStore.size() + " записей");
            return true;
        } catch (Exception e) {
            System.err.println("✗ Ошибка валидации JKS файла " + filePath + ": " + e.getMessage());
            return false;
        }
    }

    public static void main(String[] args) {

        boolean truststoreValid = validateJKS("kafka.truststore.jks", "truststore-password");
        boolean keystoreValid = validateJKS("kafka.keystore.jks", "wAUyWEpL<>tWU9VxcAipcZP");

        if (truststoreValid && keystoreValid) {
            System.out.println("Все JKS файлы валидны!");
        } else {
            System.out.println("Есть проблемы с JKS файлами!");
        }
    }
}