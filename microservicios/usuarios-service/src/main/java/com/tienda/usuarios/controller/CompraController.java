package com.tienda.usuarios.controller;

import com.tienda.eventos.Compra;
import com.tienda.usuarios.service.CompraService;
import lombok.RequiredArgsConstructor;
import org.apache.avro.util.Utf8;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/compras")
@RequiredArgsConstructor
public class CompraController {

    private final CompraService compraService;

    // public CompraController(CompraService compraService) {
    // this.compraService = compraService;
    // }

    @PostMapping
    public ResponseEntity<Map<String, String>> crearCompra(@RequestBody CompraRequest request) {
        Compra compra = new Compra();
        compra.setUserId(new Utf8(request.userId()));
        compra.setUsername(new Utf8(request.username()));
        compra.setEmail(new Utf8(request.email()));
        compra.setMonto(request.monto());
        compra.setTimestamp(Instant.now());

        compraService.publicarCompra(compra);

        Map<String, String> response = new HashMap<>();
        response.put("status", "accepted");
        response.put("message", "Compra publicada correctamente");

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    public record CompraRequest(
            String userId,
            String username,
            String email,
            Double monto) {
    }
}