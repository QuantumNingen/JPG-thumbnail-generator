import io
import multiprocessing
import os
import queue
import threading
import time
import tkinter as tk
from tkinter import filedialog

import cv2
import numpy as np
import piexif


# ==============================================================================
# --- 1. “工人”函数 ---
# ==============================================================================

def process_single_file(args):
    """
    处理单个文件的核心函数。
    返回一个包含处理状态和消息的元组, e.g., ("SUCCESS", "message text")
    """
    full_path, output_root_folder, filename_format, conflict_resolution, root_folder, _ = args
    filename = os.path.basename(full_path)

    try:
        with open(full_path, 'rb') as f:
            original_bytes = f.read()

        try:
            exif_dict = piexif.load(original_bytes)
        except Exception:
            exif_dict = {"0th": {}, "Exif": {}, "GPS": {}, "1st": {}, "thumbnail": None}

        img = cv2.imdecode(np.frombuffer(original_bytes, np.uint8), cv2.IMREAD_COLOR)
        if img is None:
            return "FAILURE", f"失败 (无法解码): {filename}"

        target_width, target_height = 160, 120
        original_height, original_width = img.shape[:2]
        ratio = min(target_width / original_width, target_height / original_height)
        new_width, new_height = int(original_width * ratio), int(original_height * ratio)
        thumb = cv2.resize(img, (new_width, new_height), interpolation=cv2.INTER_AREA)

        success, thumb_buf = cv2.imencode(".jpg", thumb)
        if not success:
            return "FAILURE", f"失败 (编码缩略图失败): {filename}"

        if "0th" not in exif_dict:
            exif_dict["0th"] = {}
        if piexif.ImageIFD.Orientation not in exif_dict["0th"]:
            exif_dict["0th"][piexif.ImageIFD.Orientation] = 0

        exif_dict["thumbnail"] = thumb_buf.tobytes()
        exif_bytes = piexif.dump(exif_dict)

        output_file_in_memory = io.BytesIO()
        piexif.insert(exif_bytes, original_bytes, output_file_in_memory)
        output_bytes = output_file_in_memory.getvalue()

        name, ext = os.path.splitext(filename)
        new_filename_base = filename_format.replace("{Filename}", name)
        new_filename = f"{new_filename_base}{ext}"

        current_folder = os.path.dirname(full_path)
        relative_path = os.path.relpath(current_folder, root_folder)
        output_folder_for_this_file = os.path.join(output_root_folder, relative_path)
        os.makedirs(output_folder_for_this_file, exist_ok=True)
        output_path = os.path.join(output_folder_for_this_file, new_filename)

        if os.path.exists(output_path):
            if conflict_resolution == "skip":
                return "SKIPPED_EXISTS", f"已跳过 (文件已存在): {new_filename}"
            elif conflict_resolution == "rename":
                name_part, ext_part = os.path.splitext(output_path)
                counter = 2
                while True:
                    new_output_path = f"{name_part} ({counter}){ext_part}"
                    if not os.path.exists(new_output_path):
                        output_path = new_output_path
                        break
                    counter += 1

        with open(output_path, 'wb') as f:
            f.write(output_bytes)

        return "SUCCESS", f"已处理: {filename} -> {os.path.basename(output_path)}"

    except Exception as e:
        return "FAILURE", f"失败: {filename} ({e})"


# ==============================================================================
# --- 2. “管理者”GUI类 ---
# ==============================================================================

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("批量为JPG图片内嵌缩略图工具v1.0（不会对图片转码）")
        self.root.geometry("600x580")
        self.status_queue = multiprocessing.Manager().Queue()

        main_frame = tk.Frame(root, padx=10, pady=10)
        main_frame.pack(fill=tk.BOTH, expand=True)

        tk.Label(main_frame, text="输入文件夹:").grid(row=0, column=0, sticky="w", pady=2)
        self.input_path_label = tk.Label(main_frame, text="尚未选择", bg="white", anchor="w", relief="sunken")
        self.input_path_label.grid(row=1, column=0, columnspan=2, sticky="ew", pady=2)
        tk.Button(main_frame, text="浏览...", command=self.select_input_folder).grid(row=1, column=2, sticky="ew",
                                                                                     padx=5)

        tk.Label(main_frame, text="输出文件夹:").grid(row=2, column=0, sticky="w", pady=2)
        self.output_path_label = tk.Label(main_frame, text="尚未选择", bg="white", anchor="w", relief="sunken")
        self.output_path_label.grid(row=3, column=0, columnspan=2, sticky="ew", pady=2)
        tk.Button(main_frame, text="浏览...", command=self.select_output_folder).grid(row=3, column=2, sticky="ew",
                                                                                      padx=5)

        tk.Label(main_frame, text="输出文件名格式 (使用 {Filename} 代表原文件名):").grid(row=4, column=0, sticky="w",
                                                                                         pady=(10, 2))
        self.filename_format_var = tk.StringVar(value="{Filename}-thumb")
        self.filename_format_entry = tk.Entry(main_frame, textvariable=self.filename_format_var)
        self.filename_format_entry.grid(row=5, column=0, columnspan=3, sticky="ew", pady=2)

        tk.Label(main_frame, text="如果输出文件已存在:").grid(row=6, column=0, sticky="w", pady=(10, 2))
        conflict_frame = tk.Frame(main_frame)
        conflict_frame.grid(row=7, column=0, columnspan=3, sticky="w")
        self.conflict_resolution_var = tk.StringVar(value="skip")
        tk.Radiobutton(conflict_frame, text="跳过", variable=self.conflict_resolution_var, value="skip").pack(
            side=tk.LEFT, padx=5)
        tk.Radiobutton(conflict_frame, text="覆盖", variable=self.conflict_resolution_var, value="overwrite").pack(
            side=tk.LEFT, padx=5)
        tk.Radiobutton(conflict_frame, text="重命名", variable=self.conflict_resolution_var, value="rename").pack(
            side=tk.LEFT, padx=5)

        perf_frame = tk.Frame(main_frame)
        perf_frame.grid(row=8, column=0, columnspan=3, sticky="w", pady=(10, 2))
        self.use_multiprocessing_var = tk.BooleanVar(value=True)
        self.multiprocessing_check = tk.Checkbutton(perf_frame, text="使用多个CPU核心",
                                                    variable=self.use_multiprocessing_var,
                                                    command=self.toggle_cpu_scaler)
        self.multiprocessing_check.pack(side=tk.LEFT)
        max_cpus = os.cpu_count() or 1
        default_cpus = min(8, max_cpus)
        self.cpu_count_var = tk.IntVar(value=default_cpus)
        self.cpu_scaler = tk.Scale(perf_frame, from_=1, to=max_cpus, orient=tk.HORIZONTAL, variable=self.cpu_count_var,
                                   length=200)
        self.cpu_scaler.pack(side=tk.LEFT, padx=10)

        self.start_button = tk.Button(main_frame, text="开始处理", command=self.start_processing_thread, height=2,
                                      bg="#4CAF50", fg="white")
        self.start_button.grid(row=9, column=0, columnspan=3, sticky="ew", pady=10)

        self.show_log_var = tk.BooleanVar(value=False)
        self.show_log_check = tk.Checkbutton(main_frame, text="实时显示处理日志 (关闭可提升刷新速度)",
                                             variable=self.show_log_var)
        self.show_log_check.grid(row=10, column=0, columnspan=3, sticky="w", pady=(5, 0))

        tk.Label(main_frame, text="处理日志:").grid(row=11, column=0, sticky="w", pady=2)
        self.status_text = tk.Text(main_frame, height=10, state="disabled", bg="#f0f0f0")
        self.status_text.grid(row=12, column=0, columnspan=3, sticky="nsew")

        main_frame.grid_columnconfigure(0, weight=1)
        main_frame.grid_rowconfigure(12, weight=1)
        self.toggle_cpu_scaler()
        self.root.after(100, self.check_queue)

    def toggle_cpu_scaler(self):
        self.cpu_scaler.config(state=tk.NORMAL if self.use_multiprocessing_var.get() else tk.DISABLED)

    def select_input_folder(self):
        folder_path = filedialog.askdirectory(title="请选择输入文件夹")
        if folder_path: self.input_path_label.config(text=folder_path)

    def select_output_folder(self):
        folder_path = filedialog.askdirectory(title="请选择输出文件夹")
        if folder_path: self.output_path_label.config(text=folder_path)

    def start_processing_thread(self):
        threading.Thread(target=self.start_processing, daemon=True).start()

    def start_processing(self):
        input_path = self.input_path_label.cget("text")
        output_path = self.output_path_label.cget("text")
        filename_format = self.filename_format_entry.get()
        conflict_resolution = self.conflict_resolution_var.get()
        show_log = self.show_log_var.get()

        if not os.path.isdir(input_path) or not os.path.isdir(output_path):
            self.status_queue.put("错误: 请输入有效的输入和输出文件夹路径。")
            return

        start_time = time.monotonic()
        self.start_button.config(state="disabled", text="正在扫描文件...")
        self.status_text.config(state="normal")
        self.status_text.delete(1.0, tk.END)
        self.status_text.config(state="disabled")

        tasks = []
        skipped_thumb_count = 0
        for current_folder, _, filenames in os.walk(input_path):
            for filename in filenames:
                if filename.lower().endswith(('.jpg', '.jpeg')):
                    full_path = os.path.join(current_folder, filename)
                    try:
                        with open(full_path, 'rb') as f:
                            exif_dict = piexif.load(f.read())
                            if exif_dict.get("thumbnail") is None:
                                tasks.append((full_path, output_path, filename_format, conflict_resolution, input_path,
                                              show_log))
                            else:
                                skipped_thumb_count += 1
                    except:
                        tasks.append(
                            (full_path, output_path, filename_format, conflict_resolution, input_path, show_log))

        self.status_queue.put(
            f"扫描完成，发现 {len(tasks)} 个文件需要处理，{skipped_thumb_count} 个文件因已有缩略图被跳过。")
        self.start_button.config(text="正在处理中...")

        use_mp = self.use_multiprocessing_var.get()
        num_processes = self.cpu_count_var.get()

        worker_args = (tasks, start_time, skipped_thumb_count)
        if use_mp:
            self.run_multiprocessing(*worker_args, num_processes)
        else:
            self.run_single_process(*worker_args)

    def process_results(self, results_iterator, start_time, skipped_thumb_count):
        processed_count, skipped_exists_count, failed_count = 0, 0, 0
        show_log = self.show_log_var.get()

        for status, message in results_iterator:
            if status == "SUCCESS":
                processed_count += 1
            elif status == "SKIPPED_EXISTS":
                skipped_exists_count += 1
            elif status == "FAILURE":
                failed_count += 1

            if show_log or status == "FAILURE":
                self.status_queue.put(message)

        duration = time.monotonic() - start_time
        summary = (
            f"\n--- 所有文件处理完毕 ---\n\n"
            f"总结:\n"
            f"  - 已处理: {processed_count} 个文件\n"
            f"  - 已跳过 (已有缩略图): {skipped_thumb_count} 个文件\n"
            f"  - 已跳过 (输出文件已存在): {skipped_exists_count} 个文件\n"
            f"  - 失败: {failed_count} 个文件\n"
            f"  - 总耗时: {duration:.3f} 秒"
        )
        self.status_queue.put(summary)

    def run_multiprocessing(self, tasks, start_time, skipped_thumb_count, num_processes):
        try:
            with multiprocessing.Pool(processes=num_processes) as pool:
                results_iterator = pool.imap_unordered(process_single_file, tasks)
                self.process_results(results_iterator, start_time, skipped_thumb_count)
        except Exception as e:
            self.status_queue.put(f"!!! 多进程错误: {e} !!!")

    def run_single_process(self, tasks, start_time, skipped_thumb_count):
        results_iterator = (process_single_file(task) for task in tasks)
        self.process_results(results_iterator, start_time, skipped_thumb_count)

    def check_queue(self):
        try:
            message = self.status_queue.get_nowait()
            self.status_text.config(state="normal")
            self.status_text.insert(tk.END, message + "\n")
            self.status_text.see(tk.END)
            self.status_text.config(state="disabled")

            if "所有文件处理完毕" in message or message.startswith("!!!") or message.startswith("错误:"):
                self.start_button.config(state="normal", text="开始处理")
        except queue.Empty:
            pass
        self.root.after(100, self.check_queue)


# ==============================================================================
# --- 3. 启动应用程序 ---
# ==============================================================================

if __name__ == "__main__":
    multiprocessing.freeze_support()
    root = tk.Tk()
    app = App(root)
    root.mainloop()