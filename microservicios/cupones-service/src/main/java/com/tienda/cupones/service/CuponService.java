package com.tienda.cupones.service;

import com.tienda.eventos.Compra;
import com.tienda.eventos.Cupon;

import lombok.RequiredArgsConstructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class CuponService {

    private static final Logger log = LoggerFactory.getLogger(CuponService.class);
    private static final double UMBRAL_MONTO = 500.0;
    private static final double DESCUENTO_DEFAULT = 50.0;

    private final KafkaTemplate<String, Cupon> kafkaTemplate;

    @Value("${app.kafka.topic-cupones}")
    private String topicCupones;

    // public CuponService(KafkaTemplate<String, Cupon> kafkaTemplate) {
    // this.kafkaTemplate = kafkaTemplate;
    // }

    public void generarCupon(Compra compra) {
        if (compra.getMonto() > UMBRAL_MONTO) {
            String codigoCupon = generarCodigo();
            Instant timestamp = Instant.now();

            Cupon cupon = new Cupon();
            cupon.setCodigo(codigoCupon);
            cupon.setUserId(compra.getUserId());
            cupon.setUsername(compra.getUsername());
            cupon.setEmail(compra.getEmail());
            cupon.setMontoCompra(compra.getMonto());
            cupon.setDescuento(DESCUENTO_DEFAULT);
            cupon.setTimestamp(timestamp);

            log.info("CUPON GENERADO: userId={}, username={}, email={}, monto={}, descuento={}, codigo={}",
                    cupon.getUserId(),
                    cupon.getUsername(),
                    cupon.getEmail(),
                    cupon.getMontoCompra(),
                    cupon.getDescuento(),
                    cupon.getCodigo());

            String key = compra.getUserId().toString();
            kafkaTemplate.send(topicCupones, key, cupon);
            log.info("Cupon enviado al topic {}: key={}", topicCupones, key);
        } else {
            log.info("SIN CUPON: monto={} <= umbral={}", compra.getMonto(), UMBRAL_MONTO);
        }
    }

    private String generarCodigo() {
        return "CUPON-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }
}