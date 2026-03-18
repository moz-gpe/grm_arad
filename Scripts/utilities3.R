#' Processar Extrações e-SISTAFE
#'
#' Esta função aplica \code{processar_esistafe_extracto_unico()} a vários
#' ficheiros de extracto do e-SISTAFE, elimina duplicações ao combinar os
#' resultados e acrescenta metadados extraídos automaticamente do nome de cada
#' ficheiro através de \code{extrair_meta_extracto()}.
#'
#' O fluxo inclui:
#' * deduplicação após o processamento dos extractos individuais;
#' * extração e anexação automática de metadados (tipo de reporte, datas, mês, ano);
#' * gestão robusta de erros (ficheiros problemáticos não interrompem o fluxo).
#'
#' ## Variáveis financeiras disponíveis no dataframe final
#'
#' O dataframe final contém métricas orçamentais padronizadas do e-SISTAFE.
#' Cada variável representa uma etapa do ciclo orçamental:
#' dotação → disponibilização → cabimentação → liquidação → pagamento.
#'
#' ### Lista de variáveis financeiras
#'
#' * **dotacao_inicial** (*Dotação Inicial*) —
#'   Alocação inicial aprovada no Orçamento do Estado.
#'
#' * **dotacao_revista** (*Dotação Revista*) —
#'   Alocação revista formalmente pelo Parlamento.
#'
#' * **dotacao_actualizada** (*Dotação Actualizada – DA*) —
#'   Atualizações feitas pelo MEF dentro dos limites legais, sem revisão parlamentar.
#'
#' * **dotacao_disponivel** (*Dotação Disponível*) —
#'   Liquidez efetivamente disponibilizada para execução.
#'
#' * **dotacao_cabimentada** (*Dotação Cabimentada – DC*) —
#'   Montante cabimentado/comprometido com base na liquidez disponível.
#'
#' * **ad_fundos** (*Adiantamento de Fundos – AF*) —
#'   Valores adiantados antes da execução; inclui, por exemplo, o mecanismo ADE para escolas.
#'
#' * **despesa_paga_via_directa** (*Despesa Paga Via Directa – DP*) —
#'   Pagamentos efetuados diretamente ao fornecedor via e-SISTAFE.
#'
#' * **ad_fundos_mais_dpvd** (*AFDP – Adiantamento de Fundos + Despesa Paga VD*) —
#'   Soma dos adiantamentos de fundos e das despesas pagas via direta.
#'
#' * **liq_ad_fundos** (*LAF – Adiantamentos de Fundos Liquidados*) —
#'   Adiantamentos para os quais já existe fatura aceite, pendentes de pagamento.
#'
#' * **despesa_liquidada_via_directa** (*LVD – Despesa Liquidada Via Directa*) —
#'   Despesas liquidadas diretamente no e-SISTAFE, com documento de liquidação emitido.
#'
#' * **liq_ad_fundos_via_directa_lafvd** (*LAFVD – Liquidação de AF + Via Directa*) —
#'   Total de liquidações combinando adiantamentos liquidados e despesas liquidadas via direta.
#'
#' @param caminhos Vetor com os caminhos completos dos ficheiros a processar.
#' @param lista_ugb Vetor de UGBs válidas a passar para \code{processar_esistafe_extracto_unico()}.
#'
#' @return Um `tibble` consolidado contendo os dados processados de todos os
#' ficheiros, incluindo metadados adicionais.
#'
#' @examples
#' \dontrun{
#' arquivos <- list.files("Data/", pattern = "\\\\.xlsx$", full.names = TRUE)
#' df <- processar_esistafe_extracto(
#'   caminhos = arquivos,
#'   lista_ugb = c("010100001", "010100003")
#' )
#' }
#'
#' @export



processar_esistafe_extracto <- function(caminhos, lista_ugb) {
  
  # -----------------------------------------------
  # Safe wrapper for processing individual files
  # -----------------------------------------------
  safe_process <- purrr::possibly(
    .f = ~{
      
      # Extract metadata using the NEW function
      meta <- extrair_meta_extracto(base::basename(.x))
      
      # Process file using the renamed main function
      df <- processar_esistafe_extracto_unico(.x, lista_ugb)
      
      # Attach metadata
      df <- df %>%
        dplyr::mutate(
          source_file   = base::basename(.x),
          fonte_reporte = meta$file_name,
          reporte_tipo  = meta$reporte_tipo,
          data_reporte  = meta$data_reporte,
          data_extraido = meta$data_extraido,
          ano           = meta$ano,
          mes           = meta$mes
        )
      
      return(df)
    },
    otherwise = dplyr::tibble()
  )
  
  # -----------------------------------------------
  # Progress bar
  # -----------------------------------------------
  pb <- progress::progress_bar$new(
    total = base::length(caminhos),
    format = "  Processing files [:bar] :percent (:current/:total) eta::eta"
  )
  
  # -----------------------------------------------
  # Apply processor to each file
  # -----------------------------------------------
  results <- purrr::map_df(caminhos, function(p) {
    pb$tick()
    safe_process(p)
  })
  
  return(results)
}



#' easystafe: Ferramentas para Processamento de Extractos do e-SISTAFE
#'
#' O pacote **easystafe** fornece funções para processamento, deduplicação,
#' extração de metadados e harmonização de extractos orçamentais provenientes
#' do sistema e-SISTAFE. Inclui um pipeline robusto para preparação de dados
#' para análise financeira, monitoria de execução e integração com workflows
#' baseados em R.
#'
#' Este pacote encontra-se atualmente em fase **experimental**.
#'
#' @docType package
#' @name easystafe
#' @keywords internal
#'
#' @section Logo:
#' \if{html}{\figure{logo.png}{options: width=120px alt="easystafe logo"}}
#' \if{latex}{\figure{logo.png}{options: width=2in}}
"_PACKAGE"

## usethis namespace: start
## usethis namespace: end

NULL




#' Extrair metadados do ficheiro e-SISTAFE
#'
#' Esta função lê o nome de um ficheiro do e-SISTAFE e extrai metadados
#' estruturados, incluindo:
#'
#' * tipo de reporte (Funcionamento, Investimento Interno, Investimento Externo);
#' * data de referência (a primeira data YYYYMMDD presente no nome);
#' * data de extração (a segunda data YYYYMMDD, se existir);
#' * ano e mês de referência, com nome do mês em Português.
#'
#' A função procura padrões \code{\\d{8}} no nome do ficheiro.
#' O primeiro padrão encontrado é assumido como data de referência
#' e o segundo como data de extração.
#'
#' @param caminho Caminho ou nome do ficheiro a partir do qual extrair metadados.
#'
#' @return Um tibble com as seguintes colunas:
#' \describe{
#'   \item{file_name}{Nome do ficheiro.}
#'   \item{reporte_tipo}{Classificação do tipo de reporte.}
#'   \item{data_reporte}{Data de referência (classe \code{Date}).}
#'   \item{data_extraido}{Data de extração (classe \code{Date}).}
#'   \item{ano}{Ano extraído da data de referência.}
#'   \item{mes}{Nome do mês (Português) correspondente à data de referência.}
#' }
#'
#' @examples
#' \dontrun{
#' extrair_meta_extracto(
#'   "OrcamentoFuncionamento_20240101_20240115.xlsx"
#' )
#' }
#'
#' @export

extrair_meta_extracto <- function(caminho) {
  
  # ------------------------------------------------------------
  # Extract file name
  # ------------------------------------------------------------
  fname <- base::basename(caminho)
  
  # ------------------------------------------------------------
  # Report type classification
  # ------------------------------------------------------------
  if (stringr::str_detect(fname, "InvestimentoCompExterna")) {
    report_type <- "Investimento Externo"
  } else if (stringr::str_detect(fname, "InvestimentoCompInterna")) {
    report_type <- "Investimento Interno"
  } else if (stringr::str_detect(fname, "OrcamentoFuncionamento")) {
    report_type <- "Funcionamento"
  } else {
    report_type <- NA_character_
  }
  
  # ------------------------------------------------------------
  # Extract dates (YYYYMMDD patterns)
  # ------------------------------------------------------------
  dates <- stringr::str_extract_all(fname, "\\d{8}")[[1]]
  
  ref_date     <- dplyr::coalesce(dates[1], NA_character_)
  extract_date <- dplyr::coalesce(dates[2], NA_character_)
  
  # Convert to real Date objects
  ref_dt     <- base::as.Date(ref_date,     format = "%Y%m%d")
  extract_dt <- base::as.Date(extract_date, format = "%Y%m%d")
  
  # ------------------------------------------------------------
  # Portuguese month names (ASCII-safe)
  # ------------------------------------------------------------
  meses_pt <- c(
    "Janeiro", "Fevereiro", "Mar\u00E7o", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
  )
  
  # ------------------------------------------------------------
  # Extract year + month
  # ------------------------------------------------------------
  ano <- if (!base::is.na(ref_dt)) lubridate::year(ref_dt) else NA_integer_
  mes <- if (!base::is.na(ref_dt)) meses_pt[lubridate::month(ref_dt)] else NA_character_
  
  # ------------------------------------------------------------
  # Return metadata tibble
  # ------------------------------------------------------------
  tibble::tibble(
    file_name     = fname,
    reporte_tipo  = report_type,
    data_reporte  = ref_dt,
    data_extraido = extract_dt,
    ano           = ano,
    mes           = mes
  )
}




#' Processar Extração Única do e-SISTAFE
#'
#' Esta função processa um ficheiro individual do e-SISTAFE, executando
#' um conjunto estruturado de operações para produzir um extracto
#' totalmente consolidado e não duplicado a partir dos diferentes níveis
#' hierárquicos de CED (A, B, C e D).
#'
#' O processamento inclui:
#' * limpeza e normalização do extracto original;
#' * classificação do grupo CED para cada linha (A, B, C, D);
#' * aplicação de regras de subtração hierárquica entre níveis:
#'   - C → D
#'   - B → D
#'   - A → D
#'   - B → C
#'   - A → C
#'   - A → B
#' * atribuição de prioridade (D > C > B > A) e seleção do valor final;
#' * devolução de um extracto “único” representando o nível mais granular disponível.
#'
#' @param caminho Caminho completo do ficheiro Excel a ser processado.
#' @param lista_ugb Vetor de códigos UGB a manter (os restantes são marcados como "Remove").
#'
#' @return Um tibble contendo o extracto processado e consolidado para um único ficheiro,
#' sem duplicações e com o nível hierárquico final escolhido automaticamente.
#'
#' @examples
#' \dontrun{
#' df <- processar_esistafe_extracto_unico(
#'   caminho = "Data/Extracto_20240201.xlsx",
#'   lista_ugb = c("010100001")
#' )
#' }
#'
#' @export

processar_esistafe_extracto_unico <- function(caminho, lista_ugb) {
  
  # ============================================================
  # --------------- LOAD & PREPARE SOURCE DATA -----------------
  # ============================================================
  
  df <- readxl::read_excel(caminho, col_type = "text") %>%
    janitor::clean_names() %>%
    dplyr::select(!dplyr::ends_with("percent")) %>%
    
    dplyr::mutate(
      dplyr::across(dotacao_inicial:liq_ad_fundos_via_directa_lafvd, as.numeric),
      ugb_id = substr(ugb, 1, 9),
      ced_base_5 = stringr::str_sub(ced, 1, 5),
      id_row_ced_5d = stringr::str_c(ugb, funcao, programa, fr, ced_base_5, sep = "_"),
      ced_base_4 = stringr::str_sub(ced, 1, 4),
      id_row_ced_4d = stringr::str_c(ugb, funcao, programa, fr, ced_base_4, sep = "_"),
      ced_base_3 = stringr::str_sub(ced, 1, 3),
      id_row_ced_3d = stringr::str_c(ugb, funcao, programa, fr, ced_base_3, sep = "_"),
      ced_base_2 = stringr::str_sub(ced, 1, 2),
      id_row_ced_2d = stringr::str_c(ugb, funcao, programa, fr, ced_base_2, sep = "_")
    ) %>%
    
    dplyr::mutate(
      mec_ugb_class  = ifelse(ugb_id %in% lista_ugb, "Keep", "Remove"),
      ced_blank_class = ifelse(!is.na(ced), "Keep", "Remove"),
      ced_group = dplyr::case_when(
        !stringr::str_ends(ced, "00")                                           ~ "A",
        stringr::str_ends(ced, "00") & !stringr::str_ends(ced, "000") & !stringr::str_ends(ced, "0000") ~ "B",
        stringr::str_ends(ced, "000") & !stringr::str_ends(ced, "0000")        ~ "C",
        stringr::str_ends(ced, "0000")                                         ~ "D",
        TRUE                                                                   ~ NA_character_
      )
    ) %>%
    
    ############################# FILTER!!!!!!
    dplyr::filter(mec_ugb_class == "Keep",
                  ced_blank_class == "Keep") %>% 
    
    dplyr::relocate(ugb_id, .before = tidyselect::everything())
  
  num_cols <- df %>% dplyr::select(where(is.numeric)) %>% names()
  
  # ============================================================
  # ------------------------ D LEVEL ----------------------------
  # ============================================================
  
  df_dc_has_c <- df %>%
    dplyr::group_by(id_row_ced_2d) %>%
    dplyr::summarise(has_C = any(ced_group == "C"), .groups = "drop")
  
  vec_dc_keep_c <- df_dc_has_c %>% dplyr::filter(has_C) %>% dplyr::pull(id_row_ced_2d)
  
  df_dc <- df %>%
    dplyr::filter(id_row_ced_2d %in% vec_dc_keep_c, ced_group %in% c("C", "D")) %>%
    dplyr::group_by(id_row_ced_3d) %>%
    dplyr::mutate(is_parent = stringr::str_ends(ced, "0000")) %>%
    dplyr::group_by(id_row_ced_2d) %>%
    dplyr::mutate(
      dplyr::across(
        tidyselect::all_of(num_cols),
        ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
      )
    ) %>%
    dplyr::ungroup()
  
  # -------------------- D fallback via B ------------------------
  
  vec_dc_no_c <- df_dc_has_c %>% dplyr::filter(!has_C) %>% dplyr::pull(id_row_ced_2d)
  
  df_db_has_b <- df %>%
    dplyr::filter(id_row_ced_2d %in% vec_dc_no_c) %>%
    dplyr::group_by(id_row_ced_2d) %>%
    dplyr::summarise(has_B = any(ced_group == "B"), .groups = "drop")
  
  vec_db_keep_b <- df_db_has_b %>% dplyr::filter(has_B) %>% dplyr::pull(id_row_ced_2d)
  
  df_db <- df %>%
    dplyr::filter(id_row_ced_2d %in% vec_db_keep_b, ced_group %in% c("B", "D")) %>%
    dplyr::group_by(id_row_ced_3d) %>%
    dplyr::mutate(is_parent = stringr::str_ends(ced, "0000")) %>%
    dplyr::group_by(id_row_ced_2d) %>%
    dplyr::mutate(
      dplyr::across(
        tidyselect::all_of(num_cols),
        ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
      )
    ) %>%
    dplyr::ungroup()
  
  # -------------------- D fallback via A ------------------------
  
  vec_db_no_b <- df_db_has_b %>% dplyr::filter(!has_B) %>% dplyr::pull(id_row_ced_2d)
  
  df_da_has_a <- df %>%
    dplyr::filter(id_row_ced_2d %in% vec_db_no_b) %>%
    dplyr::group_by(id_row_ced_2d) %>%
    dplyr::summarise(has_A = any(ced_group == "A"), .groups = "drop")
  
  vec_da_keep_a <- df_da_has_a %>% dplyr::filter(has_A) %>% dplyr::pull(id_row_ced_2d)
  
  df_da <- df %>%
    dplyr::filter(id_row_ced_2d %in% vec_da_keep_a, ced_group %in% c("A", "D")) %>%
    dplyr::group_by(id_row_ced_3d) %>%
    dplyr::mutate(is_parent = stringr::str_ends(ced, "0000")) %>%
    dplyr::group_by(id_row_ced_2d) %>%
    dplyr::mutate(
      dplyr::across(
        tidyselect::all_of(num_cols),
        ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
      )
    ) %>%
    dplyr::ungroup()
  
  df_d_final <- dplyr::bind_rows(df_dc, df_db, df_da)
  
  # ============================================================
  # ------------------------ C LEVEL ----------------------------
  # ============================================================
  
  df_cb_has_b <- df %>%
    dplyr::group_by(id_row_ced_3d) %>%
    dplyr::summarise(has_B = any(ced_group == "B"), .groups = "drop")
  
  vec_cb_keep_b <- df_cb_has_b %>% dplyr::filter(has_B) %>% dplyr::pull(id_row_ced_3d)
  
  df_cb <- df %>%
    dplyr::filter(id_row_ced_3d %in% vec_cb_keep_b, ced_group %in% c("B", "C")) %>%
    dplyr::group_by(id_row_ced_4d) %>%
    dplyr::mutate(is_parent = stringr::str_ends(ced, "000")) %>%
    dplyr::group_by(id_row_ced_3d) %>%
    dplyr::mutate(
      dplyr::across(
        tidyselect::all_of(num_cols),
        ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
      )
    ) %>%
    dplyr::ungroup()
  
  # -------------------- C fallback via A ------------------------
  
  vec_cb_no_b <- df_cb_has_b %>% dplyr::filter(!has_B) %>% dplyr::pull(id_row_ced_3d)
  
  df_ca_has_a <- df %>%
    dplyr::filter(id_row_ced_3d %in% vec_cb_no_b) %>%
    dplyr::group_by(id_row_ced_3d) %>%
    dplyr::summarise(has_A = any(ced_group == "A"), .groups = "drop")
  
  vec_ca_keep_a <- df_ca_has_a %>% dplyr::filter(has_A) %>% dplyr::pull(id_row_ced_3d)
  
  df_ca <- df %>%
    dplyr::filter(id_row_ced_3d %in% vec_ca_keep_a, ced_group %in% c("A", "C")) %>%
    dplyr::group_by(id_row_ced_4d) %>%
    dplyr::mutate(is_parent = stringr::str_ends(ced, "0000")) %>%
    dplyr::group_by(id_row_ced_3d) %>%
    dplyr::mutate(
      dplyr::across(
        tidyselect::all_of(num_cols),
        ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
      )
    ) %>%
    dplyr::ungroup()
  
  df_c_final <- dplyr::bind_rows(df_cb, df_ca)
  
  # ============================================================
  # ------------------------ B LEVEL ----------------------------
  # ============================================================
  
  df_ba_has_a <- df %>%
    dplyr::group_by(id_row_ced_4d) %>%
    dplyr::summarise(has_A = any(ced_group == "A"), .groups = "drop")
  
  vec_ba_keep_a <- df_ba_has_a %>% dplyr::filter(has_A) %>% dplyr::pull(id_row_ced_4d)
  
  df_b_final <- df %>%
    dplyr::filter(id_row_ced_4d %in% vec_ba_keep_a, ced_group %in% c("A", "B")) %>%
    dplyr::group_by(id_row_ced_5d) %>%
    dplyr::mutate(is_parent = stringr::str_ends(ced, "00")) %>%
    dplyr::group_by(id_row_ced_4d) %>%
    dplyr::mutate(
      dplyr::across(
        tidyselect::all_of(num_cols),
        ~ ifelse(is_parent, .x - sum(.x[!is_parent], na.rm = TRUE), .x)
      )
    ) %>%
    dplyr::ungroup()
  
  # ============================================================
  # ------------------------ A LEVEL ----------------------------
  # ============================================================
  
  df_a_final <- df %>%
    dplyr::filter(ced_group == "A") %>%
    dplyr::mutate(is_parent = NA)
  
  # ============================================================
  # ------------------------ PRIORITIZE --------------------------
  # ============================================================
  
  df_d_final <- df_d_final %>% dplyr::mutate(level_priority = 4)
  df_c_final <- df_c_final %>% dplyr::mutate(level_priority = 3)
  df_b_final <- df_b_final %>% dplyr::mutate(level_priority = 2)
  df_a_final <- df_a_final %>% dplyr::mutate(level_priority = 1)
  
  # ============================================================
  # ------------------------- FINALIZE ---------------------------
  # ============================================================
  
  df_final <- dplyr::bind_rows(df_d_final, df_c_final, df_b_final, df_a_final) %>%
    dplyr::arrange(id_row_ced_5d, level_priority) %>%
    dplyr::distinct(id_row_ced_5d, .keep_all = TRUE) %>%
    dplyr::select(-level_priority)
  
  return(df_final)
}
