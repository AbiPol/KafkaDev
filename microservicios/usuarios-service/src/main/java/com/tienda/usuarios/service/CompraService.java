package com.tienda.usuarios.service;

import com.tienda.eventos.Compra;

import lombok.RequiredArgsConstructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
public class CompraService {

    private static final Logger log = LoggerFactory.getLogger(CompraService.class);

    private final KafkaTemplate<String, Compra> kafkaTemplate;

    @Value("${app.kafka.topic}")
    private String topic;

    public void publicarCompra(Compra compra) {
        String key = compra.getUserId().toString();

        CompletableFuture<SendResult<String, Compra>> future = kafkaTemplate.send(topic, key, compra);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Error al publicar compra: userId={}", compra.getUserId(), ex);
            } else {
                log.info("Compra publicada: userId={}, particion={}, offset={}",
                        compra.getUserId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}