package atividade6.invoice;

import atividade6.model.Invoice;
import atividade6.model.InvoiceItem;
import com.itextpdf.kernel.pdf.PdfDocument;
import com.itextpdf.kernel.pdf.PdfWriter;
import com.itextpdf.layout.Document;
import com.itextpdf.layout.element.Paragraph;
import java.io.FileNotFoundException;
import java.util.List;

/**
 *
 * @author wagner
 */
public class PdfInvoice {

    private final Invoice nf;
    private Document documento;
    private String caminhoDocumento;

    public PdfInvoice(Invoice notaFiscal) {
        this.nf = notaFiscal;

    }

    public String gerarPdfNotaFiscal() throws FileNotFoundException {
        this.criarDocumento();
        this.definirCabecalho();
        this.definirCorpo();
        this.definirRodape();
        this.finalizarDocumento();
        return this.caminhoDocumento;
    }

    private String getCaminhoPdf() {
        String caminho = "nf-" + this.nf.number + ".pdf";
        return caminho;
    }

    private void criarDocumento() throws FileNotFoundException {
        this.caminhoDocumento = this.getCaminhoPdf();
        PdfDocument pdf = new PdfDocument(new PdfWriter(this.caminhoDocumento));
        this.documento = new Document(pdf);
        String titulo = "Nota fiscal nº " + this.nf.number;
        this.documento.add(new Paragraph(titulo));
    }

    private void definirCabecalho() {

        String linha = "----------------------------------------";
        this.documento.add(new Paragraph(linha));

        String nomeCliente = "Nome do cliente: " + this.nf.name;
        this.documento.add(new Paragraph(nomeCliente));

        String endereco = "Endereço: " + this.nf.address;
        this.documento.add(new Paragraph(endereco));

        this.documento.add(new Paragraph(linha));

    }

    private void definirCorpo() {

        List<InvoiceItem> itens = this.nf.itens;

        String titulo = "Itens da nota fiscal: (Tota: " + itens.size() + ")";
        this.documento.add(new Paragraph(titulo));
        this.documento.add(new Paragraph(""));

        //Defini variaveis de trabalho
        String codItem;
        String descricao;
        String prestador;
        String taxaDesconto;
        String qtdVlrSubtotal;
        String linha = "---";

        //Percorre os itens
        for (int i = 0; i < itens.size(); i++) {
            InvoiceItem item = itens.get(i);

            codItem = "Item: " + item.invoiceItemId;
            descricao = "Descrição: " + item.service;
            prestador = "Prestador: " + item.source + " / " + item.qualification;
            taxaDesconto = "Impostos: " + item.taxPercent + " / Desconto: " + item.discountPercent;
            qtdVlrSubtotal = "Quantidade: " + item.quantity + " / Valor unitário: " + item.unitValue + " / Subtotal: " + item.subtotal;

            this.documento.add(new Paragraph(codItem));
            this.documento.add(new Paragraph(descricao));
            this.documento.add(new Paragraph(prestador));
            this.documento.add(new Paragraph(taxaDesconto));
            this.documento.add(new Paragraph(qtdVlrSubtotal));

            this.documento.add(new Paragraph(linha));
        }

    }

    private void definirRodape() {
        this.documento.add(new Paragraph(""));
        this.documento.add(new Paragraph(""));

        String total = "Valor total: " + this.nf.value;
        this.documento.add(new Paragraph(total));
    }

    private void finalizarDocumento() {
        this.documento.close();
    }

}
