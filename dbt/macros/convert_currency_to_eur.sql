{% macro convert_currency_to_eur(foreign_currency_amount, foreign_currency) -%}
    {%- if foreign_currency == 'USD' -%}
    ROUND(SAFE_DIVIDE({{ foreign_currency_amount }}, 1.08), 2)
    {%- elif foreign_currency == 'GBP' -%}
    ROUND(SAFE_DIVIDE({{ foreign_currency_amount }}, 0.85), 2)
    {%- elif foreign_currency == 'PLN' -%}
    ROUND(SAFE_DIVIDE({{ foreign_currency_amount }}, 4.30), 2)
    {%- elif foreign_currency == 'CAD' -%}
    ROUND(SAFE_DIVIDE({{ foreign_currency_amount }}, 1.46), 2)
    {%- else -%}
    {{ foreign_currency_amount }}  -- Default case, if currency is not found in rates
    {%- endif %}
{% endmacro %}
