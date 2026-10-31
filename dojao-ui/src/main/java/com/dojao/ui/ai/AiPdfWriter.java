package com.dojao.ui.ai;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.awt.Color;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

/**
 * 最小 PDF 生成器：把一组 {@link BufferedImage} 页面（JPEG 编码后）装进 PDF 1.4，
 * 不做第三方依赖、不做字体嵌入。
 *
 * <p>每一页的内容流只有一个图像绘制指令 —— 图片以 {@code /DCTDecode} 形式内嵌，
 * 因此中文在 PDF 里的显示与渲染图片完全一致（见 {@link AiChartRenderer}）。</p>
 *
 * @author jiaxin
 * @version 1.0
 * @date 2026/10/18
 */
final class AiPdfWriter {

    private AiPdfWriter() {
    }

    /** A4 纵向 / 横向页面尺寸（单位：pt）。 */
    private static final double A4_W = 595.276;
    private static final double A4_H = 841.89;

    /** 页面四周留白（pt）。 */
    private static final double PAGE_MARGIN = 18;

    /** JPEG 压缩质量（0~1）。 */
    private static final float JPEG_QUALITY = 0.92f;

    /**
     * 把页面位图写成 PDF 字节流。
     *
     * @throws IllegalArgumentException 页面列表为空
     * @throws IllegalStateException    编码 / 写出失败
     */
    static byte[] pdf(List<BufferedImage> pages) {
        if (pages == null || pages.isEmpty()) {
            throw new IllegalArgumentException("没有可导出的页面。");
        }
        try {
            return build(pages);
        } catch (IOException e) {
            throw new IllegalStateException("生成 PDF 失败: " + e.getMessage(), e);
        }
    }

    private static byte[] build(List<BufferedImage> pages) throws IOException {
        int n = pages.size();
        int totalObjs = 2 + n * 3;
        int[] offsets = new int[totalObjs + 1];
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        writeAscii(out, "%PDF-1.4\n");
        // 二进制标记（规范建议），用 Latin-1 写出
        out.write("%\u00E2\u00E3\u00CF\u00D3\n".getBytes(StandardCharsets.ISO_8859_1));

        // 1: Catalog
        writeObj(out, offsets, 1, "<< /Type /Catalog /Pages 2 0 R >>");

        // 2: Pages
        StringBuilder kids = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                kids.append(' ');
            }
            kids.append(3 + i * 3).append(" 0 R");
        }
        writeObj(out, offsets, 2,
                "<< /Type /Pages /Count " + n + " /Kids [" + kids + "] >>");

        for (int i = 0; i < n; i++) {
            BufferedImage img = pages.get(i);
            byte[] jpeg = jpeg(img);
            int pageNo = 3 + i * 3;
            int contentNo = pageNo + 1;
            int imageNo = pageNo + 2;

            boolean landscape = img.getWidth() > img.getHeight();
            // 图表页用 A4 横向，文本 / 表格页用 A4 纵向
            double pageW = landscape ? A4_H : A4_W;
            double pageH = landscape ? A4_W : A4_H;
            double availW = pageW - 2 * PAGE_MARGIN;
            double availH = pageH - 2 * PAGE_MARGIN;
            double scale = Math.min(availW / img.getWidth(), availH / img.getHeight());
            double dw = img.getWidth() * scale;
            double dh = img.getHeight() * scale;
            double tx = (pageW - dw) / 2;
            double ty = (pageH - dh) / 2;
            String content = String.format(Locale.ROOT,
                    "q\n%.4f 0 0 %.4f %.4f %.4f cm\n/Im0 Do\nQ\n", dw, dh, tx, ty);
            byte[] contentBytes = content.getBytes(StandardCharsets.US_ASCII);

            // Page
            writeObj(out, offsets, pageNo, String.format(Locale.ROOT,
                    "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 %.3f %.3f] "
                            + "/Resources << /XObject << /Im0 %d 0 R >> >> /Contents %d 0 R >>",
                    pageW, pageH, imageNo, contentNo));

            // Contents
            offsets[contentNo] = out.size();
            writeAscii(out, contentNo + " 0 obj\n<< /Length " + contentBytes.length + " >>\nstream\n");
            out.write(contentBytes);
            writeAscii(out, "endstream\nendobj\n");

            // Image XObject（JPEG /DCTDecode）
            offsets[imageNo] = out.size();
            writeAscii(out, imageNo + " 0 obj\n"
                    + "<< /Type /XObject /Subtype /Image /Width " + img.getWidth()
                    + " /Height " + img.getHeight()
                    + " /ColorSpace /DeviceRGB /BitsPerComponent 8"
                    + " /Filter /DCTDecode /Length " + jpeg.length + " >>\nstream\n");
            out.write(jpeg);
            writeAscii(out, "\nendstream\nendobj\n");
        }

        int xrefOffset = out.size();
        writeAscii(out, "xref\n0 " + (totalObjs + 1) + "\n");
        writeAscii(out, "0000000000 65535 f \n");
        for (int i = 1; i <= totalObjs; i++) {
            writeAscii(out, String.format(Locale.ROOT, "%010d 00000 n \n", offsets[i]));
        }
        writeAscii(out, "trailer\n<< /Size " + (totalObjs + 1) + " /Root 1 0 R >>\nstartxref\n"
                + xrefOffset + "\n%%EOF\n");
        return out.toByteArray();
    }

    private static void writeObj(ByteArrayOutputStream out, int[] offsets, int num, String body) {
        offsets[num] = out.size();
        writeAscii(out, num + " 0 obj\n" + body + "\nendobj\n");
    }

    private static void writeAscii(ByteArrayOutputStream out, String s) {
        byte[] b = s.getBytes(StandardCharsets.US_ASCII);
        out.write(b, 0, b.length);
    }

    /** 转成 RGB 后按指定质量编码为 JPEG。 */
    private static byte[] jpeg(BufferedImage src) throws IOException {
        BufferedImage rgb = src;
        if (src.getType() != BufferedImage.TYPE_INT_RGB) {
            rgb = new BufferedImage(src.getWidth(), src.getHeight(), BufferedImage.TYPE_INT_RGB);
            java.awt.Graphics2D g = rgb.createGraphics();
            try {
                g.setColor(Color.WHITE);
                g.fillRect(0, 0, rgb.getWidth(), rgb.getHeight());
                g.drawImage(src, 0, 0, null);
            } finally {
                g.dispose();
            }
        }
        ImageWriter writer = ImageIO.getImageWritersByFormatName("jpeg").next();
        ImageWriteParam param = writer.getDefaultWriteParam();
        param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
        param.setCompressionQuality(JPEG_QUALITY);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ImageOutputStream ios = ImageIO.createImageOutputStream(bos)) {
            writer.setOutput(ios);
            writer.write(null, new IIOImage(rgb, null, null), param);
        } finally {
            writer.dispose();
        }
        return bos.toByteArray();
    }
}
