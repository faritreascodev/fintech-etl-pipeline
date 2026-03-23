# 📊 Fórmulas DAX Profesionales para Portafolios (Nivel Arquitecto BI)

Estas métricas son el complemento de Business Intelligence. Se montan directamente sobre tus nuevos archivos Parquet consumiendo un mínimo de RAM, usando **Time Intelligence** algorítmica.

### 1. Valor de la Cartera Actual (Último Día Cotizado)
A diferencia de sumar ventas en e-commerce, en inversiones no puedes sumar los capitales de ayer y hoy. `LASTNONBLANK` agarra únicamente la fotograma más reciente validada.

```dax
Valor Cartera Actual = 
CALCULATE (
    SUM('cartera_diaria'[valor_cartera]),
    LASTNONBLANK('cartera_diaria'[fecha], SUM('cartera_diaria'[valor_cartera]))
)
```

### 2. Rentabilidad Anual YTD (Year-to-Date)
Mide objetivamente la rentabilidad corrida en el presente año en curso.

```dax
Retorno YTD = 
VAR ValorInicial = 
    CALCULATE(
        SUM('cartera_diaria'[valor_cartera]), 
        STARTOFYEAR('cartera_diaria'[fecha])
    )
VAR ValorActual = [Valor Cartera Actual]
RETURN
DIVIDE(ValorActual - ValorInicial, ValorInicial, 0)
```

### 3. Volatilidad Dinámica a 21 Días (Riesgo Estadístico)
Añade termómetros de inestabilidad de tu portafolio usando la desviación estándar subyacente (`STDEVX.P`), ideal para detectar ventanas gráficas muy riesgosas.

```dax
Volatilidad Anualizada (21D) = 
VAR Periodo21Dias = 
    DATESINPERIOD(
        'cartera_diaria'[fecha], 
        MAX('cartera_diaria'[fecha]), 
        -21, 
        DAY
    )
VAR DesviacionDiaria = 
    STDEVX.P(
        Periodo21Dias, 
        CALCULATE(SUM('cartera_diaria'[retorno]))
    )
RETURN 
// Anualizamos penalizando diariamente (Raíz de 252 días de trading normal)
DesviacionDiaria * SQRT(252)
```

### 4. Maximum Drawdown (Caída máxima del Pico Histórico)
La pesadilla estresante de un inversor. Permite plotear una curva o un KPI mostrando qué tanto el capital ha caído porcentualmente desde su punto histórico máximo garantizado.

```dax
Max Drawdown = 
VAR MaxValorHistorico = 
    MAXX(
        FILTER(ALL('cartera_diaria'), 'cartera_diaria'[fecha] <= MAX('cartera_diaria'[fecha])), 
        [Valor Cartera Actual]
    )
VAR ValorHoy = [Valor Cartera Actual]
RETURN
DIVIDE(ValorHoy - MaxValorHistorico, MaxValorHistorico, 0)
```

### 5. Retorno Seguro (Saneamiento Excepcional)
Cálculo redundante robusto para reemplazar campos nulos en tableros donde los gráficos cruzan matrices multidimensionales vacías.

```dax
Retorno Saneado = 
IF(
    ISBLANK(SUM('cartera_diaria'[retorno])), 
    0, 
    SUM('cartera_diaria'[retorno])
)
```
