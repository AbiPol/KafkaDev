package com.tienda.cupones.service;

import com.tienda.eventos.Compra;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CompraListener {

    private static final Logger log = LoggerFactory.getLogger(CompraListener.class);

    private final CuponService cuponService;

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void escucharCompra(Compra compra) {
        log.info("Mensaje recibido: userId={}, monto={}", compra.getUserId(), compra.getMonto());
        cuponService.generarCupon(compra);
    }
}