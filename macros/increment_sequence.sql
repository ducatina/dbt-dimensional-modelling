# in macros/increment_sequence.sql

{%- macro increment_sequence() -%}
  
  {{ this.name }}_seq.nextval

{%- endmacro -%}
