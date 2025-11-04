--------------------------------------------------------
-- Archivo creado  - lunes-noviembre-03-2025   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Trigger TG_EVITAR_DOBLE_CANCELACION
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE TRIGGER "APP_OWNER"."TG_EVITAR_DOBLE_CANCELACION" 
BEFORE UPDATE ON pedidos
FOR EACH ROW
 WHEN (NEW.estado = 'cancelado' AND OLD.estado = 'cancelado') BEGIN
  RAISE_APPLICATION_ERROR(-20010, 'El pedido ya se encuentra cancelado.');
END;

/
ALTER TRIGGER "APP_OWNER"."TG_EVITAR_DOBLE_CANCELACION" ENABLE;
--------------------------------------------------------
--  DDL for Trigger TG_GUARDAR_TOTAL_PEDIDO
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE TRIGGER "APP_OWNER"."TG_GUARDAR_TOTAL_PEDIDO" 
AFTER INSERT OR UPDATE OR DELETE ON APP_OWNER.DETALLE_PEDIDOS
FOR EACH ROW
DECLARE
  v_pedido_old NUMBER;
  v_pedido_new NUMBER;
BEGIN
  -- Identificar pedido afectado (por si cambia el pedido_id en un update)
  v_pedido_old := :OLD.pedido_id;
  v_pedido_new := :NEW.pedido_id;

  -- Si es INSERT o UPDATE sobre el mismo pedido
  IF INSERTING OR (UPDATING AND v_pedido_old = v_pedido_new) THEN
    UPDATE APP_OWNER.PEDIDOS p
       SET p.total_precio =
           (SELECT NVL(ROUND(SUM(dp.cantidad * dp.precio_unitario),2),0)
              FROM APP_OWNER.DETALLE_PEDIDOS dp
             WHERE dp.pedido_id = v_pedido_new),
           p.actualizado_en = SYSTIMESTAMP
     WHERE p.id = v_pedido_new;
  END IF;

  -- Si es DELETE o UPDATE cambiando de pedido (muy raro pero cubierto)
  IF DELETING OR (UPDATING AND v_pedido_old != v_pedido_new) THEN
    UPDATE APP_OWNER.PEDIDOS p
       SET p.total_precio =
           (SELECT NVL(ROUND(SUM(dp.cantidad * dp.precio_unitario),2),0)
              FROM APP_OWNER.DETALLE_PEDIDOS dp
             WHERE dp.pedido_id = v_pedido_old),
           p.actualizado_en = SYSTIMESTAMP
     WHERE p.id = v_pedido_old;
  END IF;
END;

/
ALTER TRIGGER "APP_OWNER"."TG_GUARDAR_TOTAL_PEDIDO" ENABLE;
--------------------------------------------------------
--  DDL for Trigger TG_LIMITE_USO_CUPON
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE TRIGGER "APP_OWNER"."TG_LIMITE_USO_CUPON" 
FOR INSERT ON redenciones_cupones
COMPOUND TRIGGER

  TYPE t_set IS TABLE OF PLS_INTEGER INDEX BY VARCHAR2(200);
  g_cupones    t_set; -- set de cupones afectados (key = cupon_id)
  g_cupon_user t_set; -- set de pares cupón|usuario (key = cupon_id||'|'||usuario_id)

  AFTER EACH ROW IS
  BEGIN
    g_cupones(:NEW.cupon_id) := 1;
    g_cupon_user(:NEW.cupon_id || '|' || :NEW.usuario_id) := 1;
  END AFTER EACH ROW;

  AFTER STATEMENT IS
    k1            VARCHAR2(200);
    k2            VARCHAR2(200);
    v_cupon_id    VARCHAR2(36);
    v_usuario_id  VARCHAR2(36);
    v_max_global  NUMBER;
    v_max_usuario NUMBER;
    v_cnt         NUMBER;
  BEGIN
    -- Valida límites globales por cada cupón afectado
    k1 := g_cupones.FIRST;
    WHILE k1 IS NOT NULL LOOP
      v_cupon_id := k1;

      SELECT usos_maximos_global, usos_maximos_por_usuario
      INTO   v_max_global, v_max_usuario
      FROM   cupones
      WHERE  id = v_cupon_id
      FOR UPDATE;

      IF v_max_global IS NOT NULL THEN
        SELECT COUNT(*)
        INTO   v_cnt
        FROM   redenciones_cupones
        WHERE  cupon_id = v_cupon_id;
        IF v_cnt > v_max_global THEN
          RAISE_APPLICATION_ERROR(-20001,
            'Límite global de usos excedido ('||v_cnt||'/'||v_max_global||') para cupón '||v_cupon_id);
        END IF;
      END IF;

      -- Valida límites por usuario solo para los pares insertados de ese cupón
      IF v_max_usuario IS NOT NULL THEN
        k2 := g_cupon_user.FIRST;
        WHILE k2 IS NOT NULL LOOP
          IF SUBSTR(k2, 1, INSTR(k2,'|')-1) = v_cupon_id THEN
            v_usuario_id := SUBSTR(k2, INSTR(k2,'|')+1);
            SELECT COUNT(*)
            INTO   v_cnt
            FROM   redenciones_cupones
            WHERE  cupon_id = v_cupon_id
            AND    usuario_id = v_usuario_id;
            IF v_cnt > v_max_usuario THEN
              RAISE_APPLICATION_ERROR(-20002,
                'Límite por usuario excedido para cupón '||v_cupon_id||' y usuario '||v_usuario_id);
            END IF;
          END IF;
          k2 := g_cupon_user.NEXT(k2);
        END LOOP;
      END IF;

      k1 := g_cupones.NEXT(k1);
    END LOOP;
  END AFTER STATEMENT;

END tg_limite_uso_cupon;
/
ALTER TRIGGER "APP_OWNER"."TG_LIMITE_USO_CUPON" ENABLE;
--------------------------------------------------------
--  DDL for Trigger TG_MOVIMIENTOS_A_SALDOS
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE TRIGGER "APP_OWNER"."TG_MOVIMIENTOS_A_SALDOS" 
AFTER INSERT OR UPDATE OR DELETE ON movimientos_inventario
FOR EACH ROW
DECLARE
  v_variante_id   NUMBER;
  v_exist_diff    NUMBER := 0;
  v_reser_diff    NUMBER := 0;
BEGIN
  IF DELETING THEN
    v_variante_id := :OLD.variante_producto_id;
  ELSE
    v_variante_id := :NEW.variante_producto_id;
  END IF;

  IF DELETING THEN
    IF :OLD.tipo_movimiento IN ('reabastecimiento', 'ajuste') THEN v_exist_diff := -:OLD.cantidad;
    ELSIF :OLD.tipo_movimiento = 'venta' THEN v_exist_diff := -:OLD.cantidad; v_reser_diff := -:OLD.cantidad;
    ELSIF :OLD.tipo_movimiento = 'reserva' THEN v_reser_diff := -:OLD.cantidad;
    ELSIF :OLD.tipo_movimiento = 'reversion_cancelacion' THEN v_exist_diff := -:OLD.cantidad; v_reser_diff := +:OLD.cantidad;
    END IF;
  
  ELSIF INSERTING THEN
    IF :NEW.tipo_movimiento IN ('reabastecimiento', 'ajuste') THEN v_exist_diff := :NEW.cantidad;
    ELSIF :NEW.tipo_movimiento = 'venta' THEN v_exist_diff := :NEW.cantidad; v_reser_diff := :NEW.cantidad;
    ELSIF :NEW.tipo_movimiento = 'reserva' THEN v_reser_diff := :NEW.cantidad;
    ELSIF :NEW.tipo_movimiento = 'reversion_cancelacion' THEN v_exist_diff := :NEW.cantidad; v_reser_diff := -:NEW.cantidad;
    END IF;
    
  ELSIF UPDATING THEN
    -- Revertir OLD
    IF :OLD.tipo_movimiento IN ('reabastecimiento', 'ajuste') THEN v_exist_diff := -:OLD.cantidad;
    ELSIF :OLD.tipo_movimiento = 'venta' THEN v_exist_diff := -:OLD.cantidad; v_reser_diff := -:OLD.cantidad;
    ELSIF :OLD.tipo_movimiento = 'reserva' THEN v_reser_diff := -:OLD.cantidad;
    ELSIF :OLD.tipo_movimiento = 'reversion_cancelacion' THEN v_exist_diff := -:OLD.cantidad; v_reser_diff := +:OLD.cantidad;
    END IF;
    -- Aplicar NEW
    IF :NEW.tipo_movimiento IN ('reabastecimiento', 'ajuste') THEN v_exist_diff := v_exist_diff + :NEW.cantidad;
    ELSIF :NEW.tipo_movimiento = 'venta' THEN v_exist_diff := v_exist_diff + :NEW.cantidad; v_reser_diff := v_reser_diff + :NEW.cantidad;
    ELSIF :NEW.tipo_movimiento = 'reserva' THEN v_reser_diff := v_reser_diff + :NEW.cantidad;
    ELSIF :NEW.tipo_movimiento = 'reversion_cancelacion' THEN v_exist_diff := v_exist_diff + :NEW.cantidad; v_reser_diff := v_reser_diff - :NEW.cantidad;
    END IF;
  END IF;

  MERGE INTO saldos_inventario s
  USING (SELECT v_variante_id AS variante_id FROM DUAL) d
  ON (s.variante_producto_id = d.variante_id)
  WHEN MATCHED THEN
    UPDATE SET
      existencia = s.existencia + v_exist_diff,
      reservado = s.reservado + v_reser_diff,
      actualizado_en = SYSTIMESTAMP
    WHERE (v_exist_diff != 0 OR v_reser_diff != 0)
  WHEN NOT MATCHED THEN
    INSERT (variante_producto_id, existencia, reservado, actualizado_en)
    VALUES (v_variante_id, v_exist_diff, v_reser_diff, SYSTIMESTAMP)
    WHERE (v_exist_diff != 0 OR v_reser_diff != 0);

END;
/
ALTER TRIGGER "APP_OWNER"."TG_MOVIMIENTOS_A_SALDOS" ENABLE;
--------------------------------------------------------
--  DDL for Trigger TG_PERFILES_BI
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE TRIGGER "APP_OWNER"."TG_PERFILES_BI" 
BEFORE INSERT ON PERFILES
FOR EACH ROW
BEGIN
  IF :NEW.ID IS NULL THEN
    :NEW.ID := SEQ_PERFILES_ID.NEXTVAL;
  END IF;
END;

/
ALTER TRIGGER "APP_OWNER"."TG_PERFILES_BI" ENABLE;
--------------------------------------------------------
--  DDL for Trigger TG_UNICA_PORTADA_POR_PRODUCTO
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE TRIGGER "APP_OWNER"."TG_UNICA_PORTADA_POR_PRODUCTO" 
BEFORE INSERT OR UPDATE ON medio_producto
FOR EACH ROW
 WHEN (NEW.es_portada = 'S') DECLARE
BEGIN
    UPDATE medio_producto
    SET es_portada = 'N'
    WHERE
        producto_id = :NEW.producto_id
        AND es_portada = 'S'
        AND (
            (variante_id IS NULL AND :NEW.variante_id IS NULL) OR 
            (variante_id = :NEW.variante_id)
        )
        AND id != :NEW.id; 
END tg_unica_portada_por_producto;

/
ALTER TRIGGER "APP_OWNER"."TG_UNICA_PORTADA_POR_PRODUCTO" ENABLE;
--------------------------------------------------------
--  DDL for Procedure SP_AJUSTAR_INVENTARIO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_OWNER"."SP_AJUSTAR_INVENTARIO" (
  p_variante_producto_id  IN NUMBER,
  p_cantidad              IN NUMBER,
  p_tipo_movimiento       IN VARCHAR2,
  p_nota                  IN CLOB,
  -- Salidas
  out_movimiento_id       OUT NUMBER,
  out_nueva_existencia    OUT NUMBER,
  out_nuevo_reservado     OUT NUMBER
)
IS
  v_mov_id NUMBER;
BEGIN
  IF p_tipo_movimiento NOT IN ('ajuste', 'reabastecimiento', 'reversion_cancelacion') THEN
    RAISE_APPLICATION_ERROR(-20001, 'Tipo de movimiento debe ser ''ajuste'' o ''reabastecimiento''.');
  END IF;

  IF p_tipo_movimiento = 'reabastecimiento' AND p_cantidad <= 0 THEN
    RAISE_APPLICATION_ERROR(-20002, 'Reabastecimiento debe tener cantidad positiva.');
  END IF;

  v_mov_id := seq_movimientos_inv.NEXTVAL;

  INSERT INTO movimientos_inventario (
    id, variante_producto_id, 
    cantidad, tipo_movimiento, tipo_referencia, 
    nota
  )
  VALUES (
    v_mov_id, p_variante_producto_id,
    p_cantidad, p_tipo_movimiento, 'manual',
    p_nota
  );

  out_movimiento_id := v_mov_id;

  SELECT existencia, reservado
  INTO out_nueva_existencia, out_nuevo_reservado
  FROM saldos_inventario
  WHERE variante_producto_id = p_variante_producto_id;

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    SELECT existencia, reservado
    INTO out_nueva_existencia, out_nuevo_reservado
    FROM saldos_inventario
    WHERE variante_producto_id = p_variante_producto_id;
  WHEN OTHERS THEN
    RAISE;
END;

/
--------------------------------------------------------
--  DDL for Procedure SP_APLICAR_CUPON_A_CARRITO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_OWNER"."SP_APLICAR_CUPON_A_CARRITO" (
  p_pedido_id     IN  pedidos.id%TYPE,          -- anclado (NUMBER/VARCHAR según tu tabla)
  p_codigo_cupon  IN  cupones.codigo%TYPE,
  p_aplicado      OUT CHAR,                     -- 'S' o 'N'
  p_mensaje       OUT VARCHAR2,
  p_descuento_est OUT NUMBER
)
IS
  v_usuario_id pedidos.usuario_id%TYPE;         -- anclado
  v_cupon_id   cupones.id%TYPE;                 -- numérico (NUMBER(22))
  v_eval       t_cupon_eval;
  v_has_row    PLS_INTEGER;
  v_actual     cupones.id%TYPE;

  -- Capturar ORA-00054 (resource busy) producido por NOWAIT
  e_resource_busy EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_resource_busy, -54);
BEGIN
  ----------------------------------------------------------------------
  -- 1) Validar/lockear el pedido pendiente del usuario
  ----------------------------------------------------------------------
  SELECT usuario_id
    INTO v_usuario_id
    FROM pedidos
   WHERE id = p_pedido_id
     AND estado = 'pendiente'
   FOR UPDATE NOWAIT;

  ----------------------------------------------------------------------
  -- 2) Resolver el cupón por código (lock corto opcional)
  ----------------------------------------------------------------------
  SELECT id
    INTO v_cupon_id
    FROM cupones
   WHERE codigo = p_codigo_cupon
   FOR UPDATE NOWAIT;

  ----------------------------------------------------------------------
  -- 3) Evaluar reglas del cupón para ese pedido/usuario
  ----------------------------------------------------------------------
  v_eval := fn_cupon_aplicable(
              p_pedido_id  => p_pedido_id,
              p_cupon_id   => v_cupon_id,
              p_usuario_id => v_usuario_id
            );

  IF v_eval.es_valido = 'N' THEN
    DELETE FROM aplicaciones_cupon_carrito
     WHERE pedido_id = p_pedido_id;

    p_aplicado      := 'N';
    p_mensaje       := v_eval.razon;
    p_descuento_est := 0;
    RETURN;
  END IF;

  ----------------------------------------------------------------------
  -- 4) ¿Ya existe registro para este pedido?
  ----------------------------------------------------------------------
  SELECT COUNT(*)
    INTO v_has_row
    FROM aplicaciones_cupon_carrito
   WHERE pedido_id = p_pedido_id;

  IF v_has_row = 0 THEN
    -- 4a) Insertar
    INSERT INTO aplicaciones_cupon_carrito(
      pedido_id,
      cupon_id,                -- <-- ahora correcto (numérico)
      usuario_id,
      descuento_estimado,
      aplicado_en
    )
    VALUES(
      p_pedido_id,
      v_cupon_id,
      v_usuario_id,
      v_eval.descuento_teorico,
      SYSTIMESTAMP
    );

    p_aplicado      := 'S';
    p_mensaje       := 'Cupón aplicado.';
    p_descuento_est := v_eval.descuento_teorico;

  ELSE
    -- 4b) Toggle/Reemplazo
    SELECT cupon_id
      INTO v_actual
      FROM aplicaciones_cupon_carrito
     WHERE pedido_id = p_pedido_id
     FOR UPDATE NOWAIT;

    IF v_actual = v_cupon_id THEN
      DELETE FROM aplicaciones_cupon_carrito
       WHERE pedido_id = p_pedido_id;

      p_aplicado      := 'N';
      p_mensaje       := 'Cupón quitado.';
      p_descuento_est := 0;
    ELSE
      UPDATE aplicaciones_cupon_carrito
         SET cupon_id          = v_cupon_id,
             descuento_estimado = v_eval.descuento_teorico,
             aplicado_en        = SYSTIMESTAMP
       WHERE pedido_id = p_pedido_id;

      p_aplicado      := 'S';
      p_mensaje       := 'Cupón reemplazado.';
      p_descuento_est := v_eval.descuento_teorico;
    END IF;
  END IF;

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    -- Pedido no 'pendiente' o cupón inexistente
    p_aplicado      := 'N';
    p_mensaje       := 'Pedido no está pendiente o el cupón no existe.';
    p_descuento_est := 0;

  WHEN e_resource_busy THEN
    p_aplicado      := 'N';
    p_mensaje       := 'Recurso en uso (NOWAIT). Inténtalo nuevamente.';
    p_descuento_est := 0;

  WHEN OTHERS THEN
    p_aplicado      := 'N';
    p_mensaje       := 'Error al aplicar cupón: ' || SQLERRM;
    p_descuento_est := 0;
END;

/
--------------------------------------------------------
--  DDL for Procedure SP_CANCELAR_PEDIDO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_OWNER"."SP_CANCELAR_PEDIDO" (
  p_pedido_id       IN NUMBER,
  p_codigo_motivo   IN VARCHAR2,
  p_comentario      IN CLOB,
  out_devolucion_id         OUT NUMBER,
  out_cantidad_restaurada   OUT NUMBER
)
IS
  v_devolucion_id NUMBER;
  v_estado_actual VARCHAR2(50);
  v_total_restaurado NUMBER := 0;
  v_dummy_id_number NUMBER;
  v_dummy_exist NUMBER;
  v_dummy_reser NUMBER;
BEGIN
  BEGIN
    SELECT estado INTO v_estado_actual FROM pedidos WHERE id = p_pedido_id FOR UPDATE NOWAIT;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE_APPLICATION_ERROR(-20004, 'Pedido no encontrado.');
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20005, 'Error al bloquear pedido: ' || SQLERRM);
  END;

  IF fn_pedido_puede_transicionar(v_estado_actual, 'cancelado') = 'N' THEN
    RAISE_APPLICATION_ERROR(-20006, 'El pedido en estado "' || v_estado_actual || '" no puede ser cancelado.');
  END IF;

  INSERT INTO devoluciones (pedido_id, estado, codigo_motivo, comentario)
  VALUES (p_pedido_id, 'cancelado', p_codigo_motivo, p_comentario)
  RETURNING id INTO v_devolucion_id;

  out_devolucion_id := v_devolucion_id;

  FOR item IN (
    SELECT producto_id, 
           variante_id,
           cantidad
    FROM detalle_pedidos
    WHERE pedido_id = p_pedido_id
  ) LOOP

    INSERT INTO devolucion_items (devolucion_id, producto_id, variante_id, cantidad)
    VALUES (v_devolucion_id, item.producto_id, item.variante_id, item.cantidad);

    sp_ajustar_inventario(
      p_variante_producto_id => item.variante_id,
      p_cantidad             => item.cantidad,
      p_tipo_movimiento      => 'reversion_cancelacion',
      p_nota                 => 'Cancelación pedido ' || p_pedido_id,
      out_movimiento_id      => v_dummy_id_number,
      out_nueva_existencia   => v_dummy_exist,
      out_nuevo_reservado    => v_dummy_reser
    );

    v_total_restaurado := v_total_restaurado + item.cantidad;
  END LOOP;

  UPDATE pedidos
  SET estado = 'cancelado',
      actualizado_en = SYSTIMESTAMP
  WHERE id = p_pedido_id;

  sp_encolar_evento(
    p_tema             => 'ORDER_CANCELLED',
    p_payload_json     => '{"pedido_id": ' || p_pedido_id || ', "motivo": "' || p_codigo_motivo || '", "restaurados": ' || v_total_restaurado || '}',
    p_clave_idempotencia => 'CANCEL-' || p_pedido_id,
    out_outbox_id      => v_dummy_id_number
  );

  out_cantidad_restaurada := v_total_restaurado;

  COMMIT;

EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    RAISE;
END sp_cancelar_pedido;

/
--------------------------------------------------------
--  DDL for Procedure SP_CARRITO_ACTUALIZAR_ITEM
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_OWNER"."SP_CARRITO_ACTUALIZAR_ITEM" (
  p_carrito_id        IN  NUMBER,   
  p_producto_id       IN  NUMBER,   
  p_variante_id       IN  NUMBER,  
  p_cantidad_nueva    IN  NUMBER,
  p_nueva_cantidad    OUT NUMBER
)
IS
  v_stock_disp NUMBER;
  v_eliminado  CHAR(1);
BEGIN
  IF p_cantidad_nueva <= 0 THEN
    -- Llamada a sp_carrito_eliminar_item 
    sp_carrito_eliminar_item(p_carrito_id, p_producto_id, p_variante_id, v_eliminado);
    p_nueva_cantidad := 0;
    RETURN;
  END IF;

  -- Validar Stock (MODIFICADO NVL)
  BEGIN
    SELECT disponible INTO v_stock_disp
    FROM vw_snapshot_inventario
    WHERE producto_id = p_producto_id 
      AND NVL(variante_producto_id, 0) = NVL(p_variante_id, 0); 
  EXCEPTION 
    WHEN NO_DATA_FOUND THEN v_stock_disp := 0;
  END;

  IF p_cantidad_nueva > v_stock_disp THEN
    RAISE_APPLICATION_ERROR(-20002, 'Stock insuficiente. Disponible: ' || v_stock_disp);
  END IF;
  
  -- Actualizar cantidad 
  UPDATE cart_items
  SET cantidad = p_cantidad_nueva
  WHERE carrito_id = p_carrito_id
    AND producto_id = p_producto_id
    AND NVL(variante_id, 0) = NVL(p_variante_id, 0); 
  
  IF SQL%ROWCOUNT = 0 THEN
     RAISE_APPLICATION_ERROR(-20003, 'Item no encontrado en el carrito.');
  END IF;
  
  UPDATE carritos SET actualizado_en = SYSTIMESTAMP WHERE id = p_carrito_id;
  p_nueva_cantidad := p_cantidad_nueva;
END;

/
--------------------------------------------------------
--  DDL for Procedure SP_CARRITO_AGREGAR_ITEM
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_OWNER"."SP_CARRITO_AGREGAR_ITEM" (
  p_carrito_id        IN  NUMBER,
  p_producto_id       IN  NUMBER,
  p_variante_id       IN  NUMBER,
  p_cantidad          IN  NUMBER,
  p_nueva_cantidad    OUT NUMBER,
  p_disponible_despues OUT NUMBER
)
IS
  v_precio_obs      NUMBER(10,2);
  v_stock_disp      NUMBER;
  v_cantidad_actual NUMBER;
  v_cantidad_total  NUMBER;
BEGIN
  -- 1. Obtener precio 
  BEGIN
    SELECT precio_efectivo INTO v_precio_obs
    FROM vw_catalogo_productos
    WHERE producto_id = p_producto_id 
      AND NVL(variante_id, 0) = NVL(p_variante_id, 0) 
      AND activo = 'S';
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE_APPLICATION_ERROR(-20001, 'Producto o variante no disponible en catálogo.');
  END;

  -- 2. Obtener cantidad actual en carrito 
  BEGIN
    SELECT cantidad INTO v_cantidad_actual
    FROM cart_items
    WHERE carrito_id = p_carrito_id
      AND producto_id = p_producto_id
      AND NVL(variante_id, 0) = NVL(p_variante_id, 0); 
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      v_cantidad_actual := 0;
  END;
  
  v_cantidad_total := v_cantidad_actual + p_cantidad;

  -- 3. Validar Stock 
  BEGIN
    SELECT disponible INTO v_stock_disp
    FROM vw_snapshot_inventario
    WHERE producto_id = p_producto_id 
      AND NVL(variante_producto_id, 0) = NVL(p_variante_id, 0);
  EXCEPTION 
    WHEN NO_DATA_FOUND THEN v_stock_disp := 0;
  END;
  
  IF v_cantidad_total > v_stock_disp THEN
    p_nueva_cantidad := v_cantidad_actual;
    p_disponible_despues := v_stock_disp;
    RAISE_APPLICATION_ERROR(-20002, 'Stock insuficiente. Disponible: ' || v_stock_disp);
  END IF;

  -- 4. Hacer UPSERT (MERGE) del item 
  MERGE INTO cart_items TGT
  USING (SELECT 1 FROM dual)
  ON (TGT.carrito_id = p_carrito_id AND 
      TGT.producto_id = p_producto_id AND
      NVL(TGT.variante_id, 0) = NVL(p_variante_id, 0)) 
  WHEN MATCHED THEN
    UPDATE SET TGT.cantidad = v_cantidad_total, 
               TGT.precio_unitario_observado = v_precio_obs
  WHEN NOT MATCHED THEN
    INSERT (carrito_id, producto_id, variante_id, cantidad, precio_unitario_observado)
    VALUES (p_carrito_id, p_producto_id, p_variante_id, p_cantidad, v_precio_obs); 

  -- 5. Actualizar timestamp del carrito
  UPDATE carritos SET actualizado_en = SYSTIMESTAMP WHERE id = p_carrito_id;

  -- Salidas
  p_nueva_cantidad := v_cantidad_total;
  p_disponible_despues := v_stock_disp - v_cantidad_total;

END;

/
--------------------------------------------------------
--  DDL for Procedure SP_CARRITO_APLICAR_CUPON
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_OWNER"."SP_CARRITO_APLICAR_CUPON" (
  p_carrito_id    IN  NUMBER,   
  p_codigo_cupon  IN  VARCHAR2,
  p_aplicado      OUT CHAR,
  p_mensaje       OUT VARCHAR2,
  p_descuento_est OUT NUMBER
)
IS
  v_cupon_id      cupones.id%TYPE; 
  v_cupon         cupones%ROWTYPE; 
  v_subtotal      NUMBER;
  v_estimacion    t_estimacion_carrito;
BEGIN
  p_aplicado := 'N';
  p_descuento_est := 0;

  -- 1. Si el código es nulo o vacío, quitar cupón
  IF p_codigo_cupon IS NULL OR p_codigo_cupon = '' THEN
    UPDATE carritos SET cupon_id = NULL, actualizado_en = SYSTIMESTAMP
    WHERE id = p_carrito_id;
    p_aplicado := 'N';
    p_mensaje := 'Cupón quitado.';
    RETURN;
  END IF;

  -- 2. Obtener datos del cupón
  BEGIN
    SELECT * INTO v_cupon
    FROM cupones
    WHERE codigo = p_codigo_cupon;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      p_mensaje := 'Cupón no existe.';
      RETURN;
  END;

  -- 3. Obtener subtotal del carrito (usando la vista)
  SELECT subtotal_estimado INTO v_subtotal
  FROM vw_detalle_carrito
  WHERE carrito_id = p_carrito_id;

  -- 4. Validar cupón
  IF v_cupon.activo <> 'S' THEN
    p_mensaje := 'Cupón inactivo.';
  ELSIF v_cupon.vigente_desde IS NOT NULL AND v_cupon.vigente_desde > SYSTIMESTAMP THEN
    p_mensaje := 'Cupón aún no es válido.';
  ELSIF v_cupon.vigente_hasta IS NOT NULL AND v_cupon.vigente_hasta < SYSTIMESTAMP THEN
    p_mensaje := 'Cupón ha vencido.';
  ELSIF v_cupon.subtotal_minimo IS NOT NULL AND v_subtotal < v_cupon.subtotal_minimo THEN
    p_mensaje := 'No se alcanza el subtotal mínimo requerido.';
  ELSE
    -- Cupón es válido
    UPDATE carritos SET cupon_id = v_cupon.id, actualizado_en = SYSTIMESTAMP
    WHERE id = p_carrito_id;

    p_aplicado := 'S';
    p_mensaje := 'Cupón aplicado exitosamente.';

    -- Recalcular descuento
    v_estimacion := fn_estimaciones_carrito(p_carrito_id);
    p_descuento_est := v_estimacion.descuento_estimado;
    RETURN;
  END IF;

  -- Si llega aquí, falló la validación; desasociar cupón
  UPDATE carritos SET cupon_id = NULL, actualizado_en = SYSTIMESTAMP
  WHERE id = p_carrito_id;

END;

/
--------------------------------------------------------
--  DDL for Procedure SP_CARRITO_ELIMINAR_ITEM
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_OWNER"."SP_CARRITO_ELIMINAR_ITEM" (
  p_carrito_id    IN  NUMBER,   
  p_producto_id   IN  NUMBER, 
  p_variante_id   IN  NUMBER,
  p_eliminado     OUT CHAR
)
IS
BEGIN
  DELETE FROM cart_items
  WHERE carrito_id = p_carrito_id
    AND producto_id = p_producto_id
    AND NVL(variante_id, 0) = NVL(p_variante_id, 0);
    
  IF SQL%ROWCOUNT > 0 THEN
    p_eliminado := 'S';
    UPDATE carritos SET actualizado_en = SYSTIMESTAMP WHERE id = p_carrito_id;
  ELSE
    p_eliminado := 'N';
  END IF;
END;

/
--------------------------------------------------------
--  DDL for Procedure SP_CHECKOUT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_OWNER"."SP_CHECKOUT" (
  p_carrito_id       IN  APP_OWNER.CARRITOS.ID%TYPE,
  p_ubicacion_id     IN  APP_OWNER.PEDIDOS.LOCAL_ID%TYPE,
  o_pedido_id        OUT APP_OWNER.PEDIDOS.ID%TYPE,
  o_cantidad_items   OUT NUMBER,
  o_total_general    OUT NUMBER,
  o_evento_outbox_id OUT NUMBER
) IS
  -- Datos del carrito
  v_perfil_id   APP_OWNER.CARRITOS.PERFIL_ID%TYPE;
  v_cupon_id    APP_OWNER.CARRITOS.CUPON_ID%TYPE;

  -- Acumuladores
  v_items NUMBER := 0;
  v_total NUMBER := 0;

  v_now   TIMESTAMP WITH TIME ZONE := SYSTIMESTAMP;

  -- Cursor sobre los ítems del carrito (bloqueados para evitar carreras)
  CURSOR c_items IS
    SELECT id, producto_id, variante_id, cantidad, precio_unitario_observado
    FROM   APP_OWNER.CART_ITEMS
    WHERE  carrito_id = p_carrito_id
    FOR UPDATE;
BEGIN
  ------------------------------------------------------------
  -- 1) Validaciones y bloqueo del carrito
  ------------------------------------------------------------
  IF p_carrito_id IS NULL THEN
    RAISE_APPLICATION_ERROR(-20010, 'carrito_id es requerido');
  END IF;

  -- Bloquea la fila del carrito y trae PERFIL_ID/CUPON_ID
  BEGIN
    SELECT perfil_id, cupon_id
      INTO v_perfil_id, v_cupon_id
    FROM APP_OWNER.CARRITOS
    WHERE id = p_carrito_id
    FOR UPDATE;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE_APPLICATION_ERROR(-20011, 'Carrito no existe');
  END;

  ------------------------------------------------------------
  -- 2) Recorrer items y calcular totales
  ------------------------------------------------------------
  v_items := 0; v_total := 0;

  FOR r IN c_items LOOP
    IF r.cantidad IS NULL OR r.cantidad <= 0 THEN
      RAISE_APPLICATION_ERROR(-20012, 'Cantidad inválida en item '||r.id);
    END IF;
    IF r.precio_unitario_observado IS NULL OR r.precio_unitario_observado < 0 THEN
      RAISE_APPLICATION_ERROR(-20013, 'Precio inválido en item '||r.id);
    END IF;

    v_items := v_items + 1;
    v_total := v_total + (r.cantidad * r.precio_unitario_observado);
  END LOOP;

  IF v_items = 0 THEN
    RAISE_APPLICATION_ERROR(-20014, 'El carrito está vacío');
  END IF;

  ------------------------------------------------------------
  -- 3) Crear PEDIDO (estado: pagado)
  ------------------------------------------------------------
  INSERT INTO APP_OWNER.PEDIDOS
    (FECHA_PEDIDO, FECHA_RETIRO, ESTADO, TOTAL_PRECIO,
     NOTAS, CREADO_EN, ACTUALIZADO_EN, USUARIO_ID, LOCAL_ID)
  VALUES
    (v_now, v_now + INTERVAL '45' MINUTE, 'pagado', ROUND(v_total,2),
     NULL,  v_now,  v_now, v_perfil_id, p_ubicacion_id)
  RETURNING ID INTO o_pedido_id;

  ------------------------------------------------------------
  -- 4) Crear DETALLE_PEDIDOS desde CART_ITEMS
  ------------------------------------------------------------
  INSERT INTO APP_OWNER.DETALLE_PEDIDOS
    (CANTIDAD, PRECIO_UNITARIO, PRODUCTO_ID, PEDIDO_ID, VARIANTE_ID)
  SELECT
    ci.cantidad,
    ci.precio_unitario_observado,
    ci.producto_id,
    o_pedido_id,
    ci.variante_id
  FROM APP_OWNER.CART_ITEMS ci
  WHERE ci.carrito_id = p_carrito_id;

  ------------------------------------------------------------
  -- 5) Redención de cupón (si aplica)
  ------------------------------------------------------------
  IF v_cupon_id IS NOT NULL THEN
    INSERT INTO APP_OWNER.REDENCIONES_CUPONES
      (REDIMIDO_EN, USUARIO_ID, PEDIDO_ID, CUPON_ID)
    VALUES
      (v_now, v_perfil_id, o_pedido_id, v_cupon_id);
  END IF;

  ------------------------------------------------------------
  -- 6) Cerrar carrito y limpiar sus ítems
  ------------------------------------------------------------
  UPDATE APP_OWNER.CARRITOS
     SET ESTADO = 'cerrado',
         ACTUALIZADO_EN = v_now
   WHERE ID = p_carrito_id;

  DELETE FROM APP_OWNER.CART_ITEMS
   WHERE CARRITO_ID = p_carrito_id;

  o_cantidad_items := v_items;
  o_total_general  := ROUND(v_total, 2);

  ------------------------------------------------------------
  -- 7) Encolar evento en WEBHOOK_BANDEJA_SALIDA
  --    (ID autogenerado: usamos RETURNING)
  ------------------------------------------------------------
  INSERT INTO APP_OWNER.WEBHOOK_BANDEJA_SALIDA
    (TEMA, PAYLOAD, CLAVE_IDEMPOTENCIA, ESTADO, REINTENTOS, CREADO_EN, ACTUALIZADO_EN)
  VALUES
    ('ORDER_CHECKED_OUT',
     '{"pedido_id": '||o_pedido_id||', "perfil_id": '||NVL(v_perfil_id,0)||'}',
     'pedido:'||o_pedido_id,
     'pendiente',
     0,
     v_now,
     v_now)
  RETURNING id INTO o_evento_outbox_id;

  COMMIT;

EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    RAISE;
END SP_CHECKOUT;

/
--------------------------------------------------------
--  DDL for Procedure SP_CREAR_O_FUSIONAR_CARRITO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_OWNER"."SP_CREAR_O_FUSIONAR_CARRITO" (
  p_perfil_id         IN  NUMBER,  
  p_token_sesion      IN  VARCHAR2, 
  p_carrito_id        OUT NUMBER,  
  p_fusionado         OUT CHAR,
  p_carrito_previo_id OUT NUMBER    
)
IS
  v_user_cart_id   NUMBER := NULL; 
  v_anon_cart_id   NUMBER := NULL; 
BEGIN
  p_fusionado := 'N';
  p_carrito_previo_id := NULL;

  -- 1. Buscar carrito activo del usuario (si está logueado)
  IF p_perfil_id IS NOT NULL THEN
    BEGIN
      SELECT id INTO v_user_cart_id
      FROM carritos
      WHERE perfil_id = p_perfil_id AND estado = 'activo'
      FETCH FIRST 1 ROWS ONLY;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN v_user_cart_id := NULL;
    END;
  END IF;

  -- 2. Buscar carrito activo anónimo (si hay token)
  IF p_token_sesion IS NOT NULL THEN
    BEGIN
      SELECT id INTO v_anon_cart_id
      FROM carritos
      WHERE token_sesion = p_token_sesion AND estado = 'activo'
      FETCH FIRST 1 ROWS ONLY;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN v_anon_cart_id := NULL;
    END;
  END IF;

  -- 3. Decidir lógica
  
  -- Caso A: Usuario logueado, sin carrito anónimo
  IF v_user_cart_id IS NOT NULL AND v_anon_cart_id IS NULL THEN
    p_carrito_id := v_user_cart_id;
    RETURN;
  
  -- Caso B: Carrito anónimo existe, sin carrito de usuario
  ELSIF v_user_cart_id IS NULL AND v_anon_cart_id IS NOT NULL THEN
    p_carrito_id := v_anon_cart_id;
    IF p_perfil_id IS NOT NULL THEN
      UPDATE carritos
      SET perfil_id = p_perfil_id, token_sesion = NULL, actualizado_en = SYSTIMESTAMP
      WHERE id = p_carrito_id;
    END IF;
    RETURN;

  -- Caso C: No existe ningún carrito 
  ELSIF v_user_cart_id IS NULL AND v_anon_cart_id IS NULL THEN
    INSERT INTO carritos (perfil_id, token_sesion, estado)
    VALUES (p_perfil_id, p_token_sesion, 'activo')
    RETURNING id INTO p_carrito_id; 
    RETURN;

  -- Caso D: FUSIÓN. 
  ELSIF v_user_cart_id IS NOT NULL AND v_anon_cart_id IS NOT NULL THEN
    p_carrito_id := v_user_cart_id;
    p_fusionado := 'S';
    p_carrito_previo_id := v_anon_cart_id;

    MERGE INTO cart_items TGT
    USING (SELECT producto_id, variante_id, cantidad, precio_unitario_observado 
           FROM cart_items WHERE carrito_id = v_anon_cart_id) SRC
    ON (TGT.carrito_id = p_carrito_id AND 
        TGT.producto_id = SRC.producto_id AND
        NVL(TGT.variante_id, 0) = NVL(SRC.variante_id, 0)) 
    WHEN MATCHED THEN
      UPDATE SET TGT.cantidad = TGT.cantidad + SRC.cantidad
    WHEN NOT MATCHED THEN
      INSERT (carrito_id, producto_id, variante_id, cantidad, precio_unitario_observado) 
      VALUES (p_carrito_id, SRC.producto_id, SRC.variante_id, SRC.cantidad, SRC.precio_unitario_observado);
    
    UPDATE carritos SET estado = 'abandonado', actualizado_en = SYSTIMESTAMP
    WHERE id = v_anon_cart_id;
    RETURN;
  END IF;
  
END;

/
--------------------------------------------------------
--  DDL for Procedure SP_DEFINIR_PORTADA
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_OWNER"."SP_DEFINIR_PORTADA" (
    p_producto_id IN productos.id%TYPE,
    p_medio_id_nuevo IN medio_archivo.id%TYPE,
    p_portada_anterior_id OUT medio_archivo.id%TYPE
) AS v_medio_producto_anterior_id medio_producto.id%TYPE;

BEGIN
    BEGIN 
        SELECT id, medio_id
        INTO v_medio_producto_anterior_id, p_portada_anterior_id
        FROM medio_producto
        WHERE producto_id = p_producto_id AND es_portada = 'S' AND variante_id IS NULL
        FETCH FIRST 1 ROWS ONLY;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_medio_producto_anterior_id := NULL;
            p_portada_anterior_id := NULL;
    END;
    IF v_medio_producto_anterior_id IS NOT NULL THEN
        UPDATE medio_producto
        SET es_portada = 'N'
        WHERE id = v_medio_producto_anterior_id;
    END IF;

    UPDATE medio_producto
    SET es_portada = 'S',
        orden = -1
    WHERE producto_id = p_producto_id AND medio_id = p_medio_id_nuevo AND variante_id IS NULL;

    COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
END sp_definir_portada;

/
--------------------------------------------------------
--  DDL for Procedure SP_ENCOLAR_EVENTO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_OWNER"."SP_ENCOLAR_EVENTO" (
  p_tema              IN VARCHAR2,
  p_payload_json      IN CLOB,
  p_clave_idempotencia IN VARCHAR2,
  out_outbox_id       OUT NUMBER
)
IS
  v_id NUMBER;
BEGIN
  INSERT INTO webhook_bandeja_salida (tema, payload, clave_idempotencia, estado)
  VALUES (p_tema, p_payload_json, p_clave_idempotencia, 'pendiente')
  RETURNING id INTO v_id;

  out_outbox_id := v_id;

EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
    SELECT id INTO out_outbox_id
    FROM webhook_bandeja_salida
    WHERE clave_idempotencia = p_clave_idempotencia;
END sp_encolar_evento;

/
--------------------------------------------------------
--  DDL for Procedure SP_FINALIZAR_REDENCION_CUPON
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_OWNER"."SP_FINALIZAR_REDENCION_CUPON" (
  p_pedido_id        IN  VARCHAR2,
  p_cupon_id         IN  VARCHAR2,
  p_usuario_id       IN  VARCHAR2,
  p_redencion_creada OUT CHAR
)
IS
  v_eval            t_cupon_eval;
  v_dummy           CHAR(1);          -- <-- ahora en el bloque externo
  v_usuario_pedido  VARCHAR2(36);
BEGIN
  -- Bloqueo del cupón para serializar contra otras sesiones
  SELECT 'X' INTO v_dummy
  FROM cupones
  WHERE id = p_cupon_id
  FOR UPDATE NOWAIT;

  -- Idempotencia según la PK (pedido_id, cupon_id)
  BEGIN
    SELECT 'X' INTO v_dummy
    FROM redenciones_cupones
    WHERE pedido_id = p_pedido_id
      AND cupon_id  = p_cupon_id;
    p_redencion_creada := 'N'; -- ya existe la redención para ese pedido+cupón
    RETURN;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;
  END;

  -- Coherencia del pedido y usuario (opcional pero recomendable)
  BEGIN
    SELECT usuario_id INTO v_usuario_pedido
    FROM pedidos
    WHERE id = p_pedido_id;
    IF v_usuario_pedido <> p_usuario_id THEN
      p_redencion_creada := 'N';
      RETURN;
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      p_redencion_creada := 'N';
      RETURN;
  END;

  -- (Opcional) Revalidación: si finalizas antes de cambiar estado funciona tal cual;
  -- si finalizas DESPUÉS de cambiar a 'pagado/completado', quita esta llamada o
  -- crea una variante que no exija estado 'pendiente'.
  v_eval := fn_cupon_aplicable(
              p_pedido_id   => p_pedido_id,
              p_cupon_id    => p_cupon_id,
              p_usuario_id  => p_usuario_id
            );
  IF v_eval.es_valido = 'N' THEN
    p_redencion_creada := 'N';
    RETURN;
  END IF;

  -- Inserta redención
  INSERT INTO redenciones_cupones(cupon_id, pedido_id, usuario_id)
  VALUES (p_cupon_id, p_pedido_id, p_usuario_id);

  -- Limpia aplicación temporal si existiera
  DELETE FROM aplicaciones_cupon_carrito WHERE pedido_id = p_pedido_id;

  p_redencion_creada := 'S';

EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
    p_redencion_creada := 'N'; -- otra sesión se nos adelantó
  WHEN OTHERS THEN
    p_redencion_creada := 'N';
END;

/
--------------------------------------------------------
--  DDL for Procedure SP_RECALCULAR_TOTAL_PEDIDO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_OWNER"."SP_RECALCULAR_TOTAL_PEDIDO" (
  p_pedido_id       IN  APP_OWNER.PEDIDOS.ID%TYPE,
  o_total_anterior  OUT NUMBER,
  o_total_nuevo     OUT NUMBER,
  o_delta           OUT NUMBER
) IS
BEGIN
  -------------------------------------------------
  -- 1) Obtener total anterior (y bloquear la fila)
  -------------------------------------------------
  SELECT total_precio
    INTO o_total_anterior
    FROM APP_OWNER.PEDIDOS
   WHERE id = p_pedido_id
   FOR UPDATE;

  -------------------------------------------------
  -- 2) Recalcular desde las líneas de pedido
  -------------------------------------------------
  SELECT NVL(ROUND(SUM(dp.cantidad * dp.precio_unitario), 2), 0)
    INTO o_total_nuevo
    FROM APP_OWNER.DETALLE_PEDIDOS dp
   WHERE dp.pedido_id = p_pedido_id;

  -------------------------------------------------
  -- 3) Actualizar si cambió
  -------------------------------------------------
  UPDATE APP_OWNER.PEDIDOS
     SET total_precio  = o_total_nuevo,
         actualizado_en = SYSTIMESTAMP
   WHERE id = p_pedido_id;

  -------------------------------------------------
  -- 4) Delta de diferencia
  -------------------------------------------------
  o_delta := o_total_nuevo - o_total_anterior;

  COMMIT;

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20021, 'No existe el pedido '||p_pedido_id);
  WHEN OTHERS THEN
    ROLLBACK;
    RAISE;
END SP_RECALCULAR_TOTAL_PEDIDO;

/
--------------------------------------------------------
--  DDL for Procedure SP_RECONSTRUIR_SALDOS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_OWNER"."SP_RECONSTRUIR_SALDOS" (
  out_filas_actualizadas OUT NUMBER,
  out_fecha_corte        OUT TIMESTAMP WITH TIME ZONE
)
IS
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  out_fecha_corte := SYSTIMESTAMP;

  EXECUTE IMMEDIATE 'ALTER TRIGGER tg_movimientos_a_saldos DISABLE';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE saldos_inventario';

  INSERT INTO saldos_inventario (
    variante_producto_id, 
    existencia, 
    reservado, 
    actualizado_en
  )
  SELECT
    variante_producto_id,
    -- Suma de existencia
    SUM(CASE 
          WHEN tipo_movimiento IN ('reabastecimiento', 'ajuste', 'reversion_cancelacion') THEN cantidad
          WHEN tipo_movimiento = 'venta' THEN cantidad
          ELSE 0 
        END) AS calc_existencia,
    -- Suma de reservados
    SUM(CASE 
          WHEN tipo_movimiento = 'reserva' THEN cantidad
          WHEN tipo_movimiento = 'venta' THEN cantidad
          WHEN tipo_movimiento = 'reversion_cancelacion' THEN -cantidad
          ELSE 0 
        END) AS calc_reservado,
    out_fecha_corte
  FROM 
    movimientos_inventario
  GROUP BY 
    variante_producto_id;
  
  out_filas_actualizadas := SQL%ROWCOUNT;

  EXECUTE IMMEDIATE 'ALTER TRIGGER tg_movimientos_a_saldos ENABLE';

  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    EXECUTE IMMEDIATE 'ALTER TRIGGER tg_movimientos_a_saldos ENABLE';
    RAISE;
END;

/
--------------------------------------------------------
--  DDL for Function FN_CUPON_APLICABLE
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "APP_OWNER"."FN_CUPON_APLICABLE" (
  p_pedido_id   IN VARCHAR2,
  p_cupon_id    IN VARCHAR2,
  p_usuario_id  IN VARCHAR2
) RETURN t_cupon_eval
IS
  v_activo            CHAR(1);
  v_tipo              VARCHAR2(20);
  v_valor             NUMBER(10,2);
  v_min_subtotal      NUMBER(10,2);
  v_desde             TIMESTAMP WITH TIME ZONE;
  v_hasta             TIMESTAMP WITH TIME ZONE;
  v_max_global        NUMBER;
  v_max_usuario       NUMBER;

  v_subtotal          NUMBER(18,2);
  v_now               TIMESTAMP WITH TIME ZONE := SYSTIMESTAMP;

  v_usos_global       NUMBER;
  v_usos_usuario      NUMBER;

  v_desc_teorico      NUMBER(18,2);
BEGIN
  -- Pedido mínimo: que exista y esté pendiente
  DECLARE v_dummy NUMBER; v_estado VARCHAR2(50); v_usuario_pedido VARCHAR2(36);
  BEGIN
    SELECT 1, estado, usuario_id INTO v_dummy, v_estado, v_usuario_pedido
    FROM pedidos WHERE id = p_pedido_id;
    IF v_estado <> 'pendiente' THEN
      RETURN t_cupon_eval('N','El pedido no está editable (no es pendiente).',0,0);
    END IF;
    -- opcional: validar que el usuario coincida
    IF v_usuario_pedido <> p_usuario_id THEN
      RETURN t_cupon_eval('N','El pedido no pertenece al usuario.',0,0);
    END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RETURN t_cupon_eval('N','Pedido inexistente.',0,0);
  END;

  -- Cupon
  BEGIN
    SELECT activo, tipo_descuento, valor_descuento, subtotal_minimo,
           vigente_desde, vigente_hasta, usos_maximos_global, usos_maximos_por_usuario
    INTO v_activo, v_tipo, v_valor, v_min_subtotal, v_desde, v_hasta, v_max_global, v_max_usuario
    FROM cupones
    WHERE id = p_cupon_id;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RETURN t_cupon_eval('N','Cupón inexistente.',0,0);
  END;

  IF v_activo <> 'S' THEN
    RETURN t_cupon_eval('N','Cupón inactivo.',0,0);
  END IF;

  IF v_desde IS NOT NULL AND v_now < v_desde THEN
    RETURN t_cupon_eval('N','Cupón fuera de vigencia (aún no inicia).',0,0);
  END IF;
  IF v_hasta IS NOT NULL AND v_now > v_hasta THEN
    RETURN t_cupon_eval('N','Cupón fuera de vigencia (ya venció).',0,0);
  END IF;

  SELECT NVL(SUM(cantidad * precio_unitario),0)
  INTO v_subtotal
  FROM detalle_pedidos
  WHERE pedido_id = p_pedido_id;

  IF v_subtotal <= 0 THEN
    RETURN t_cupon_eval('N','Carrito vacío.',0,0);
  END IF;

  IF v_min_subtotal IS NOT NULL AND v_subtotal < v_min_subtotal THEN
    RETURN t_cupon_eval('N','No cumple subtotal mínimo.',0,0);
  END IF;

  SELECT COUNT(*) INTO v_usos_global
  FROM redenciones_cupones
  WHERE cupon_id = p_cupon_id;

  SELECT COUNT(*) INTO v_usos_usuario
  FROM redenciones_cupones
  WHERE cupon_id = p_cupon_id AND usuario_id = p_usuario_id;

  IF v_max_global IS NOT NULL AND v_usos_global >= v_max_global THEN
    RETURN t_cupon_eval('N','Sin cupos globales disponibles.',0,0);
  END IF;
  IF v_max_usuario IS NOT NULL AND v_usos_usuario >= v_max_usuario THEN
    RETURN t_cupon_eval('N','Límite por usuario alcanzado.',0,0);
  END IF;

  IF v_tipo = 'monto' THEN
    v_desc_teorico := LEAST(v_subtotal, v_valor);
  ELSE
    v_desc_teorico := ROUND(v_subtotal * (v_valor/100), 2);
  END IF;

  -- Hook opcional con fn_precio_con_descuentos(...) aquí si existiera
  RETURN t_cupon_eval('S','Aplicable.',v_desc_teorico,v_desc_teorico);
END;

/
--------------------------------------------------------
--  DDL for Function FN_ESTIMACIONES_CARRITO
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "APP_OWNER"."FN_ESTIMACIONES_CARRITO" (
  p_carrito_id IN NUMBER -- MODIFICADO
) RETURN t_estimacion_carrito
IS
  v_estimacion t_estimacion_carrito := t_estimacion_carrito(0, 0, 0, 0);
BEGIN
  -- Usamos la vista vw_detalle_carrito que ya tiene esta lógica
  SELECT cantidad_items, subtotal_estimado, descuento_estimado, total_estimado
  INTO v_estimacion.cantidad_items, v_estimacion.subtotal_estimado, 
       v_estimacion.descuento_estimado, v_estimacion.total_estimado
  FROM vw_detalle_carrito
  WHERE carrito_id = p_carrito_id;

  RETURN v_estimacion;
  
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    -- Si el carrito no existe, devuelve un objeto vacío (0,0,0,0)
    RETURN v_estimacion;
END;

/
--------------------------------------------------------
--  DDL for Function FN_PEDIDO_PUEDE_TRANSICIONAR
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "APP_OWNER"."FN_PEDIDO_PUEDE_TRANSICIONAR" (
  p_estado_desde IN VARCHAR2,
  p_estado_hacia IN VARCHAR2
) RETURN CHAR
IS
BEGIN
  CASE p_estado_desde
    WHEN 'pendiente' THEN
      IF p_estado_hacia IN ('pagado', 'cancelado') THEN RETURN 'S'; END IF;
    WHEN 'pagado' THEN
      IF p_estado_hacia IN ('enviado', 'cancelado') THEN RETURN 'S'; END IF; 
    WHEN 'enviado' THEN
      IF p_estado_hacia IN ('completado') THEN RETURN 'S'; END IF;
    WHEN 'completado' THEN
      RETURN 'N';
    WHEN 'cancelado' THEN
      RETURN 'N';
  END CASE;
  RETURN 'N';
END fn_pedido_puede_transicionar;

/
--------------------------------------------------------
--  DDL for Function FN_PRECIO_CON_DESCUENTOS
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "APP_OWNER"."FN_PRECIO_CON_DESCUENTOS" (
    p_producto_id IN productos.id%TYPE,
    p_variante_id IN variantes_producto.id%TYPE,
    p_cupon_id IN cupones.id%TYPE
) RETURN NUMBER
    IS
        v_precio_base NUMBER(10,2);
        v_precio_final NUMBER (10,2);
        v_tipo_descuento cupones.tipo_descuento%TYPE;
        v_valor_descuento cupones.valor_descuento%TYPE;
        v_monto_descuento NUMBER(10,2) := 0;
BEGIN
    --- Precio base ----
    SELECT NVL(v.precio_sobrescrito, p.precio)
    INTO v_precio_base
    FROM productos p
    LEFT JOIN variantes_producto v
    ON p.id = v.producto_id AND v.id = p_variante_id
    WHERE p.id = p_producto_id;

    --- Descuento ----
    IF p_cupon_id IS NOT NULL THEN
        BEGIN
            SELECT tipo_descuento, valor_descuento
            INTO v_tipo_descuento, v_valor_descuento
            FROM cupones
            WHERE id = p_cupon_id AND activo = 'S' 
                AND (vigente_desde IS NULL OR vigente_desde <= SYSTIMESTAMP)
                AND (vigente_hasta IS NULL OR vigente_hasta >= SYSTIMESTAMP);
            IF v_tipo_descuento = 'monto' THEN
                v_monto_descuento := v_valor_descuento;
            ELSIF v_tipo_descuento = 'porcentaje' THEN
                v_monto_descuento := v_precio_base * (v_valor_descuento/100);
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_monto_descuento := 0;
        END;
    END IF;
    --- Precio base ----
    v_precio_final := v_precio_base - v_monto_descuento;

    IF v_precio_final < 0 THEN
        v_precio_final := 0;
    END IF;

    RETURN v_precio_final;

    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
END fn_precio_con_descuentos;

/
--------------------------------------------------------
--  DDL for Function FN_SIGUIENTE_NUMERO_PEDIDO
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "APP_OWNER"."FN_SIGUIENTE_NUMERO_PEDIDO" (
  p_prefijo IN VARCHAR2,
  p_fecha   IN DATE
) RETURN VARCHAR2
IS
  PRAGMA AUTONOMOUS_TRANSACTION;

  v_prefijo  VARCHAR2(16) := NVL(TRIM(p_prefijo), 'GEN');
  v_fecha    DATE         := NVL(p_fecha, SYSDATE);
  v_anio     VARCHAR2(4);
  v_seq      NUMBER;
  v_numero   VARCHAR2(64);
BEGIN
  -- Normalizar prefijo: MAYÚSCULAS y sólo A-Z, 0-9, _ y -
  v_prefijo := REGEXP_REPLACE(UPPER(v_prefijo), '[^A-Z0-9_-]', '');
  IF v_prefijo IS NULL THEN
    v_prefijo := 'GEN';
  END IF;

  v_anio := TO_CHAR(v_fecha, 'YYYY');

  -- Obtener NEXTVAL; si la secuencia no existe, crearla on the fly
  BEGIN
    EXECUTE IMMEDIATE 'SELECT APP_OWNER.SEQ_DOC_NUM.NEXTVAL FROM DUAL'
      INTO v_seq;
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE = -2289 THEN  -- ORA-02289: sequence does not exist
        EXECUTE IMMEDIATE q'[
          CREATE SEQUENCE APP_OWNER.SEQ_DOC_NUM
            START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE
        ]';
        EXECUTE IMMEDIATE 'SELECT APP_OWNER.SEQ_DOC_NUM.NEXTVAL FROM DUAL'
          INTO v_seq;
      ELSE
        RAISE;
      END IF;
  END;

  -- Construir número legible: PREFIJO-YYYY-000001
  v_numero := v_prefijo || '-' || v_anio || '-' || LPAD(v_seq, 6, '0');

  COMMIT;  -- Reserva del número (transacción autónoma)
  RETURN v_numero;
END;

/
--------------------------------------------------------
--  DDL for Function FN_STOCK_DISPONIBLE
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "APP_OWNER"."FN_STOCK_DISPONIBLE" (
  p_variante_producto_id   IN NUMBER
) RETURN NUMBER
IS
  v_disponible NUMBER := 0;
BEGIN
  SELECT (existencia - reservado)
  INTO v_disponible
  FROM saldos_inventario
  WHERE variante_producto_id = p_variante_producto_id;

  RETURN v_disponible;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN 0;
END;

/
